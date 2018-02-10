import logging, sys

from multiprocessing import Pool
from lxml import etree
from urllib import request
from datetime import datetime
from datetime import timedelta

import pandas as pd

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)


def _parse_data_node(node):
    columns = list()
    data = list()
    for child in node.getchildren():
        if child.tag == "metadata":
            metadata_children = child.getchildren()
            for metadata_child in metadata_children:
                if metadata_child.tag == "columns":
                    for columns_child in metadata_child.getchildren():
                        if columns_child.attrib and columns_child.attrib.get("name") is not None:
                            columns.append(columns_child.attrib.get("name"))
        if child.tag == "rows":
            rows = child.getchildren()
            for row in rows:
                row_for_data = list()
                data.append(row_for_data)
                for column in columns:
                    if column != 'DATE':
                        row_for_data.append(row.attrib.get(column))

    return pd.DataFrame(data, columns=columns)


def _xml_to_df(xml, *target_child_id):
    '''
    :return: Return list of data frames
    '''
    frames = list()
    for node in xml.getchildren():
        if node.tag == 'data':
            if not target_child_id or node.attrib and node.attrib.get("id") in target_child_id:
                frames.append(_parse_data_node(node))

    return frames


def _sanitize(dataframe):
    for column in dataframe.columns:
        dataframe[column] = dataframe[column].apply(lambda value: None if value == '' else value)

    return dataframe


def _load_url(url):
    logging.debug("url='%s'", url)
    text = request.urlopen(url).read()

    return text


class MOEX(object):
    """
    ISS MOEX API: http://iss.moex.com/iss/reference/
    """
    def __init__(self, processes=None) -> None:
        super().__init__()
        self.__pool = Pool(processes=processes)

    def securities(self, q=None, limit=100, lang='ru'):
        """
        URL: https://iss.moex.com/iss/reference/5\n
        :return: Pandas DataFrame
        """
        base_url = "https://iss.moex.com/iss/securities.xml"
        base_url = "{base_url}?q={query}".format(base_url=base_url, query=q) if q is not None else base_url
        return _xml_to_df(etree.fromstring(_load_url(base_url)), "securities")[0]

    def securities_aggregates(self, security):
        '''
        URL: https://iss.moex.com/iss/reference/214\n
        :return: Return Pandas DataFrame
        '''

        base_url = "https://iss.moex.com/iss/securities/{security}/aggregates.xml"
        return _xml_to_df(etree.fromstring(_load_url(base_url.format(security=security))), "securities")[0]


    def securities_indicies(self, security, only_actual=0, lang='ru'):
        """
        URL: https://iss.moex.com/iss/reference/160\n
        :return: Pandas DataFrame
        """
        base_url = "https://iss.moex.com/iss/securities/{security}/indices.xml?only_actual={only_actual}&lang={lang}".format(
            security=security, only_actual=only_actual, lang=lang)
        return _xml_to_df(etree.fromstring(_load_url(base_url)), "indices")[0]

    " https://iss.moex.com/iss/turnovers.xml"

    def index(self):

        base_url = "https://iss.moex.com/iss/index.xml"
        return _xml_to_df(etree.fromstring(_load_url(base_url)))

    def turnovers(self):
        """
        URL: https://iss.moex.com/iss/reference/24\n
        :return: Pandas DataFrame
        """
        base_url = "https://iss.moex.com/iss/turnovers.xml"
        children = ["turnovers", "turnoversprevdate", "turnoverssectors", "turnoverssectorsprevdate"]
        return _xml_to_df(etree.fromstring(_load_url(base_url)), children)

    def enginies(self, engine=None):
        """
        URL: https://iss.moex.com/iss/reference/40\n
        :return: Pandas DataFrame
        """
        base_url = "https://iss.moex.com/iss/engines.xml" if engine is None else "https://iss.moex.com/iss/engines/{engine}.xml".format(engine=engine)
        return _xml_to_df(etree.fromstring(_load_url(base_url)))

    def statistics_engines_futures_markets_indicativerates_securities(self, date_start=None, date_end=None):
        """
        URL: https://iss.moex.com/iss/reference/146\n
        :return: Pandas DataFrame
        """
        return None

    def history_engines_stock_totals_securities(self, secid, columns=list(), date_start=None, date_end=None):
        """
        URL /iss/history/engines/stock/totals/securities
        :return: Pandas DataFrame
        """
        result = pd.DataFrame([])

        date_start = datetime.strptime(date_start,
                                       "%Y-%m-%d").date() if date_start is not None else datetime.now().date()

        date_end = datetime.strptime(date_end, "%Y-%m-%d").date() if date_end is not None else date_start
        days = max((date_end - date_start).days, 1)

        def mk_url(date):
            base_url = 'https://iss.moex.com/iss/history/engines/stock/totals/securities.xml'
            url = base_url + '?date=' + date if date is not None else base_url
            return url

        urls = [mk_url((date_start + timedelta(days=day)).strftime("%Y-%m-%d")) for day in range(0, days)]

        for document in self.__pool.map(_load_url, urls):
            df = _xml_to_df(etree.fromstring(document), "securities")[0]
            df = df[df["BOARDID"] == "MRKT"]
            df = df[df["SECID"].isin(secid)]
            df = df[columns]
            result = pd.concat([result, df])

        result.reset_index(drop=True, inplace=True)

        return result

    def statistics_engines_stock_markets_index_analytics(self, indexid=None, date=datetime.now().strftime('%Y-%m-%d'),
                                                         limit=20, start=0, lang='ru'):
        """
        URL: https://iss.moex.com/iss/reference/160\n
        :return: Pandas DataFrame
        """
        if indexid is None:
            base_url = "https://iss.moex.com//iss/statistics/engines/stock/markets/index/analytics.xml?lang={lang}".format(
                lang=lang)
            return _xml_to_df(etree.fromstring(_load_url(base_url)), "indices")[0]
        else:
            base_url = "https://iss.moex.com//iss/statistics/engines/stock/markets/index/analytics/{indexid}.xml"
            parameters = \
                "lang={lang}&" \
                "limit={limit}&" \
                "start={start}&" \
                "date={date}".format(lang=lang, limit=limit, start=start, date=date)
            base_url = base_url + "?" + parameters
            return _xml_to_df(etree.fromstring(_load_url(base_url)), "indices")[0]

    def statistics_engines_stock_capitalization(self, date_start=None, date_end=None):
        """
        URL: https://iss.moex.com/iss/reference/159\n
        :return: Pandas DataFrame
        """

        result = pd.DataFrame([])

        date_start = datetime.strptime(date_start,
                                       "%Y-%m-%d").date() if date_start is not None else datetime.now().date()

        date_end = datetime.strptime(date_end, "%Y-%m-%d").date() if date_end is not None else date_start
        days = max((date_end - date_start).days, 1)

        def mk_url(date):
            base_url = 'https://iss.moex.com/iss/statistics/engines/stock/capitalization.xml'
            url = base_url + '?date=' + date if date is not None else base_url
            return url

        urls = [mk_url((date_start + timedelta(days=day)).strftime("%Y-%m-%d")) for day in range(0, days)]

        for document in self.__pool.map(_load_url, urls):
            frames = _xml_to_df(etree.fromstring(document), "capitalization")
            result = pd.concat([result, frames[0] if len(frames) > 0 else pd.DataFrame([])])

        result.reset_index(drop=True, inplace=True)
        for column in result.columns:
            result[column] = result[column].apply(lambda value: None if value == '' else value)
        result.dropna(inplace=True)
        return result

    def history_engines_markets_securities(self, engine, market, boardgroup, *securities, date_start=None, date_end=None, limit=100, start=0, security_collection=None):
        '''
        URL: https://iss.moex.com/iss/reference/29\n
        :return: Pandas DataFrame
        '''
        base_url = "https://iss.moex.com/iss/history/engines/{engine}/markets/{market}/boardgroups/{boardgroup}/securities.xml".format(engine=engine, market=market, boardgroup=boardgroup)
        base_url = base_url + "?limit={limit}&start={start}&date={date}&securities={securities}"
        base_url = base_url + "&security_collection=" + str(security_collection) if security_collection is not None else base_url

        result = pd.DataFrame([])

        date_start = datetime.strptime(date_start,
                                       "%Y-%m-%d").date() if date_start is not None else datetime.now().date()

        date_end = datetime.strptime(date_end, "%Y-%m-%d").date() if date_end is not None else date_start
        days = max((date_end - date_start).days, 1)

        dates = [(date_start + timedelta(days=day)).strftime("%Y-%m-%d") for day in range(0, days)]
        securities = str(securities).replace('(', '').replace(')', '').replace("'", '').replace(' ', '')
        urls = [base_url.format(limit=limit, start=start, date=date, securities=securities) for date in dates]

        for document in self.__pool.map(_load_url, urls):
            frames = _xml_to_df(etree.fromstring(document), "history")
            result = pd.concat([result, frames[0] if len(frames) > 0 else pd.DataFrame([])])

        result.reset_index(drop=True, inplace=True)

        _sanitize(result)

        # result.dropna(inplace=True)

        return result

    def engines_markets_boardgroups(self, engine='stock', market='index'):
        '''
        URL: https://iss.moex.com/iss/reference/45\n
        :return: Pandas DataFrame
        '''
        base_url = "https://iss.moex.com/iss/engines/{engine}/markets/{market}/boardgroups.xml".format(engine=engine, market=market)

        return _xml_to_df(etree.fromstring(_load_url(base_url)), "boardgroups")[0]


if __name__ == "__main__":
    moex = MOEX()

    data = moex.history_engines_stock_totals_securities(date_start='2018-01-01',
                                                 secid=['SBER', 'MOEX', 'SNGS', 'AFLT', 'GAZP', 'LKOH', 'GMKN', 'MGNT',
                                                        'ROSN', 'TATN', 'MTSS', 'VTBR', 'ALRS', 'IRAO'])

    print(data)