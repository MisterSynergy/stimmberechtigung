"""
Author:   https://de.wikipedia.org/wiki/Benutzer:MisterSynergy
License:  MIT license
Version:  2023-09-14
Task:     update statistics related to “Allgemeine Stimmberechtigung” in German Wikipedia
See also: https://de.wikipedia.org/wiki/Benutzer:MisterSynergy/Stimmberechtigung
"""

from datetime import datetime, timedelta
from math import ceil as math_ceil
from os.path import expanduser
from time import gmtime, strftime, time
from typing import Any, Optional, Union

import mariadb
import matplotlib.pyplot as plt
import pandas as pd
import pywikibot as pwb


NS0_EDITS_ALL = 200
NS0_EDITS_MINOR = 50
MINOR_TIME = 1 # years
FIRST_EDIT_TIME = 2 # months

SITE = pwb.Site(code='de', fam='wikipedia')
SUBPAGE_TITLE_BASE = 'Benutzer:MsynBot/Stimmberechtigung/'
REPORT_PAGE_TITLE = 'Benutzer:MisterSynergy/Stimmberechtigung'

REPORT_TEMPLATE = './report.template'
STATISTICS_TEMPLATE = './statistics_table.template'


class Replica:
    def __init__(self) -> None:
        self.replica = mariadb.connect(
            host='dewiki.analytics.db.svc.wikimedia.cloud',
            database='dewiki_p',
            default_file=f'{expanduser("~")}/replica.my.cnf'
        )
        self.cursor = self.replica.cursor(dictionary=True)

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.replica.close()


class Plot:
    def __init__(self, filename:Optional[str]=None, getfig:bool=False, nrows:int=1, ncols:int=1, \
                 figsize:Optional[tuple[float, float]]=None, svg:bool=True):
        self.filename = filename
        self.getfig = getfig
        if figsize is None:
            figsize = (6.4, 4.8)
        self.fig, self.ax = plt.subplots(nrows=nrows, ncols=ncols, figsize=figsize)
        self.svg = svg

    def __enter__(self):
        if self.getfig is True:
            return (self.fig, self.ax)

        return self.ax

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.fig.tight_layout()
        if self.filename is not None:
            self.fig.savefig(f'./{self.filename}.png')
            if self.svg is True:
                self.fig.savefig(f'./{self.filename}.svg')
        plt.close(self.fig)


def query_dewiki(query:str) -> list[dict[str, Any]]:
    with Replica() as db_cursor:
        db_cursor.execute(query)
        result = db_cursor.fetchall()

    return result


def query_dewiki_to_dataframe(query:str) -> pd.DataFrame:
    result = query_dewiki(query)

    df = pd.DataFrame(
        data=result
    )

    return df


def first_query(minor_timestamp:int) -> pd.DataFrame:
    query = f"""SELECT
  user_id,
  CONVERT(user_name USING utf8) AS user_name,
  user_editcount,
  CONVERT(user_registration USING utf8) AS user_registration,
  COUNT(rev_id) AS user_editcount_ns0_last_year
FROM
  revision_userindex
    JOIN page ON rev_page=page_id
    JOIN actor_revision ON rev_actor=actor_id
    JOIN user ON actor_user=user_id
WHERE
  page_namespace=0
  AND rev_timestamp>{minor_timestamp:d}
  AND user_editcount>={NS0_EDITS_ALL:d}
GROUP BY
  user_id,
  user_editcount,
  user_registration
HAVING
  user_editcount_ns0_last_year>={NS0_EDITS_MINOR:d}"""

    return query_dewiki_to_dataframe(query)


def second_query() -> pd.DataFrame:
    query = 'SELECT ug_user FROM user_groups WHERE ug_group="bot"'

    return query_dewiki_to_dataframe(query)


def third_query(first_edit_timestamp:int) -> pd.DataFrame:
    query = f"""SELECT
  DISTINCT user_id
FROM
  revision_userindex
    JOIN actor_revision ON actor_id=rev_actor
    JOIN user ON actor_user=user_id
WHERE
  user_editcount>={NS0_EDITS_ALL:d}
  AND rev_timestamp<={first_edit_timestamp:d}"""

    return query_dewiki_to_dataframe(query)


def fourth_query() -> pd.DataFrame:
    query = f"""SELECT
  user_id,
  COUNT(rev_id) AS user_editcount_ns0_all_time
FROM
  revision_userindex
    JOIN page ON rev_page=page_id
    JOIN actor_revision ON actor_id=rev_actor
    JOIN user ON actor_user=user_id
WHERE
  page_namespace=0
  AND user_editcount>={NS0_EDITS_ALL:d}
GROUP BY
  user_id"""

    return query_dewiki_to_dataframe(query)


def get_first_timestamp(user_id:int) -> pd._libs.tslibs.timestamps.Timestamp:
    query = f"""SELECT
      CONVERT(MIN(rev_timestamp) USING utf8) AS min_rev_timestamp
    FROM
      revision_userindex
        JOIN actor_revision ON rev_actor=actor_id
    WHERE
      actor_user={user_id:d}"""

    result = query_dewiki(query)

    if len(result) == 0 or len(result[0]) == 0:
        raise RuntimeWarning()

    dt = pd.to_datetime(
        arg=int(result[0].get('min_rev_timestamp', 0)),
        format='%Y%m%d%H%M%S'
    )

    return dt


def get_pseudo_registration(tpl:tuple[int, pd._libs.tslibs.timestamps.Timestamp]) -> pd._libs.tslibs.timestamps.Timestamp:
    user_id, registration = tpl
    if not isinstance(registration, pd._libs.tslibs.nattype.NaTType):
        return registration

    return get_first_timestamp(user_id)


def calc_minor_timestamp_classical() -> int:
    return int(f'{int(strftime("%Y"))-MINOR_TIME:4d}{strftime("%m%d%H%M%S")}')


def calc_minor_timestamp_precise(ts:float) -> int:
    t_now = datetime.fromtimestamp(ts)
    delta = timedelta(days=MINOR_TIME*365)

    return int((t_now - delta).strftime('%Y%m%d%H%M%S'))


def calc_first_edit_timestamp_classical() -> int:
    first_edit_month = int(strftime('%m')) - FIRST_EDIT_TIME
    first_edit_year = int(strftime('%Y'))
    if first_edit_month < 1:
        first_edit_month += 12
        first_edit_year -= 1
    first_edit_timestamp = int(f'{first_edit_year:4d}{first_edit_month:02d}{strftime("%d%H%M%S")}')

    return first_edit_timestamp


def calc_first_edit_timestamp_precise(ts:float) -> int:
    t_now = datetime.fromtimestamp(ts)
    delta = timedelta(days=FIRST_EDIT_TIME*30)

    return int((t_now - delta).strftime('%Y%m%d%H%M%S'))


def save_to_wiki(page_title:str, wikitext:str, append:bool=False) -> None:
    page = pwb.Page(SITE, page_title)
    if append is True:
        page.text += wikitext
    else:
        page.text = wikitext

    #print(page.text)
    page.save(
        summary='Bot: aktualisiere Statistiken zur Allgemeinen Stimmberechtigung #msynbot',
        watch='nochange',
        minor=True,
        quiet=True
    )


def get_final_dataframe(t_start:float, dump_df:bool=False, verbose:bool=False) -> pd.DataFrame:
    minor_timestamp = calc_minor_timestamp_precise(t_start)
    first_edit_timestamp = calc_first_edit_timestamp_precise(t_start)

    #### first query
    df1 = first_query(minor_timestamp)
    t_query1 = time()
    if verbose is True:
        print(f'Found {df1.shape[0]} users with {NS0_EDITS_MINOR}+ ns0 edits during past {MINOR_TIME}' \
              f' year(s), and {NS0_EDITS_ALL}+ edits in total ({t_query1-t_start:.0f} s)')

    #### second query
    df2 = second_query()
    t_query2 = time()
    if verbose is True:
        print(f'Found {df2.shape[0]} users with botflag ({t_query2-t_query1:.0f} s)')

    #### third query
    df3 = third_query(first_edit_timestamp)
    t_query3 = time()
    if verbose is True:
        print(f'Found {df3.shape[0]} users with first edit more than {FIRST_EDIT_TIME} month(s) ago,' \
              f' and {NS0_EDITS_ALL}+ edits in total ({t_query3-t_query2:.0f} s)')

    #### fourth query
    df4 = fourth_query()
    if verbose is True:
        print(f'Found {df4.shape[0]} users with {NS0_EDITS_ALL}+ ns0 edits ({time()-t_query3:.0f} s)')

    #### combine everything into a final dataframe
    users_with_stimmberechtigung = df1.loc[~df1['user_id'].isin(df2['ug_user'])].merge(
        right=df3,
        on='user_id'
    ).merge(
        right=df4.loc[df4['user_editcount_ns0_all_time']>=NS0_EDITS_ALL],
        on='user_id'
    )

    if dump_df is True:
        users_with_stimmberechtigung.to_csv(
            f'./logs/result_{int(time())}.tsv',
            sep='\t'
        )

    users_with_stimmberechtigung['registration'] = pd.to_datetime(
        users_with_stimmberechtigung['user_registration'],
        format='%Y%m%d%H%M%S'
    )

    users_with_stimmberechtigung['pseudo_registration'] = users_with_stimmberechtigung[
        [ 'user_id', 'registration' ]
    ].apply(
        axis=1,
        func=get_pseudo_registration
    )

    if verbose is True:
        print('\n# of accounts with "Allgemeine Stimmberechtigung":' \
             f' {users_with_stimmberechtigung.shape[0]} ({time()-t_start:.0f} s)')

    return users_with_stimmberechtigung


def get_final_dataframe_testing() -> pd.DataFrame:
    users_with_stimmberechtigung = pd.read_csv(
        './logs/result_1636404820.0846128.tsv',
        sep='\t',
        header=0,
        names=[
            'unnamed',
            'user_id',
            'user_name',
            'user_editcount',
            'user_registration',
            'user_editcount_ns0_last_year',
            'user_editcount_ns0_all_time'
        ],
        dtype={
            'unnamed' : int,
            'user_id' : int,
            'user_name' : str,
            'user_editcount' : int,
            'user_registration' : float,
            'user_editcount_ns0_last_year' : int,
            'user_editcount_ns0_all_time' : int
        }
    )

    users_with_stimmberechtigung['registration'] = pd.to_datetime(
        users_with_stimmberechtigung['user_registration'],
        format='%Y%m%d%H%M%S'
    )

    return users_with_stimmberechtigung


def accounts_by_registration_year(df:pd.DataFrame, dump_df_to_wiki:bool=True, save_image:bool=False) -> None:
    tmp = df['user_id'].groupby(df['pseudo_registration'].dt.year).count().reset_index()

    if dump_df_to_wiki is True:
        wikitext_data = 'year,cnt,series\n'
        for elem in tmp.itertuples():
            wikitext_data += f'{int(elem.pseudo_registration):d},{elem.user_id},"accounts_by_year"\n'

        save_to_wiki(f'{SUBPAGE_TITLE_BASE}account_registration_year/data', wikitext_data)

    if save_image is True:
        with Plot(filename='accounts_by_registration_year') as ax:
            tmp.plot(x='pseudo_registration', y='user_id', kind='scatter', ax=ax)

            ax.set_xlabel('Anmeldejahr')
            ax.set_ylabel('Accounts mit "Allgemeiner Stimmberechtigung"')
            _, xmax, _, ymax = ax.axis()
            tick_years = 3
            ax.set_xticks(range(2001, math_ceil(xmax/tick_years)*tick_years+1, tick_years))
            ax.set(xlim=(2000, xmax), ylim=(0, ymax*1.1))


def accounts_by_editcount(df:pd.DataFrame, dump_df_to_wiki:bool=True, save_image:bool=False) -> None:
    tmp = df['user_id'].groupby(df['user_editcount']).count().reset_index()

    if dump_df_to_wiki is True:
        wikitext = 'editcount,cnt,series\n'
        for elem in tmp.itertuples():
            wikitext += f'{elem.user_editcount},{elem.user_id},"user_editcount"\n'

        save_to_wiki(f'{SUBPAGE_TITLE_BASE}account_editcount/data', wikitext)

    if save_image is True:
        with Plot('accounts_by_editcount') as ax:
            tmp.plot(x='user_editcount', y='user_id', kind='scatter', ax=ax, logy=True, logx=True)

            ax.set_xlabel('Beitragszahl')
            ax.set_ylabel('Accounts mit "Allgemeiner Stimmberechtigung"')


def accounts_by_editcount_ns0(df:pd.DataFrame, dump_df_to_wiki:bool=True, save_image:bool=False) -> None:
    tmp = df['user_id'].groupby(df['user_editcount_ns0_all_time']).count().reset_index()

    if dump_df_to_wiki is True:
        wikitext = 'editcount,cnt,series\n'
        for elem in tmp.itertuples():
            wikitext += f'{elem.user_editcount_ns0_all_time},{elem.user_id},"user_editcount_ns0_all_time"\n'

        save_to_wiki(f'{SUBPAGE_TITLE_BASE}account_editcount_ns0/data', wikitext)

    if save_image is True:
        with Plot(filename='accounts_by_editcount_ns0') as ax:
            tmp.plot(x='user_editcount_ns0_all_time', y='user_id', kind='scatter', ax=ax, logy=True, logx=True)

            ax.set_xlabel('Beitragszahl (Hauptnamensraum)')
            ax.set_ylabel('Accounts mit "Allgemeiner Stimmberechtigung"')


def accounts_by_editcount_ns0_past_year(df:pd.DataFrame, dump_df_to_wiki:bool=True, save_image:bool=False) -> None:
    tmp = df['user_id'].groupby(df['user_editcount_ns0_last_year']).count().reset_index()

    if dump_df_to_wiki is True:
        wikitext = 'ecitcount,cnt,series\n'
        for elem in tmp.itertuples():
            wikitext += f'{elem.user_editcount_ns0_last_year},{elem.user_id},"user_editcount_ns0_last_year"\n'

        save_to_wiki(f'{SUBPAGE_TITLE_BASE}account_editcount_ns0_past_year/data', wikitext)

    if save_image is True:
        with Plot(filename='accounts_by_editcount_ns0_past_year') as ax:
            tmp.plot(x='user_editcount_ns0_last_year', y='user_id', kind='scatter', ax=ax, logy=True, logx=True)

            ax.set_xlabel('Beitragszahl (Hauptnamensraum, letztes Jahr)')
            ax.set_ylabel('Accounts mit "Allgemeiner Stimmberechtigung"')


def append_current_value(t_start:float, df:pd.DataFrame) -> None:
    wikitext = f'\n{strftime("%Y/%m/%d", gmtime(t_start))},{df.shape[0]},"grtv"'
    save_to_wiki(f'{SUBPAGE_TITLE_BASE}time_series/data', wikitext, append=True)


def get_statistics_data(df:pd.DataFrame) -> dict[str, Union[float, int, str]]:
    keys = [
        'user_editcount',
        'user_editcount_ns0_last_year',
        'user_editcount_ns0_all_time',
        'pseudo_registration'
    ]

    statistics:dict[str, Union[float, int, str]] = {}
    for key in keys:
        s = str(df[key].describe(datetime_is_numeric=True))
        for line in s.split('\n'):
            if line.startswith('Name: '):
                continue

            measure, value = line.split(maxsplit=1)

            if key in [ 'registration', 'pseudo_registration' ]:
                if measure == 'count':
                    statistics[f'{key}_{measure}'] = int(float(value))
                else:
                    statistics[f'{key}_{measure}'] = value[:10]
            else:
                if measure in [ 'mean', 'std' ]:
                    statistics[f'{key}_{measure}'] = float(value)
                else:
                    statistics[f'{key}_{measure}'] = int(float(value))

    return statistics


def get_misc_statistics(df:pd.DataFrame) -> str:
    with open(STATISTICS_TEMPLATE, mode='r', encoding='utf8') as file_handle:
        template = file_handle.read()

    statistics_data = get_statistics_data(df)
    table_wikitext = template.format(**statistics_data)

    return table_wikitext


def update_main_report(df:pd.DataFrame, t_start:float) -> None:
    with open(REPORT_TEMPLATE, mode='r', encoding='utf8') as file_handle:
        template = file_handle.read()

    params:dict[str, str] = {
        'cnt' : str(df.shape[0]),
        'timestamp' : str(int(t_start)),
        'timestamp_formatted' : strftime('%-d. %B %Y, %-H:%M:%S (UTC)', gmtime(t_start)),
        'registration_unknown' : str(df.loc[df['registration'].isna()].shape[0]),
        'registration_in_2005' : str(df.loc[df['registration'].dt.year==2005].shape[0]),
        'table_wikitext' : get_misc_statistics(df)
    }
    wikitext = template.format(**params)

    save_to_wiki(REPORT_PAGE_TITLE, wikitext)


def main() -> None:
    t_start = time()
    users_with_stimmberechtigung = get_final_dataframe(t_start, dump_df=True, verbose=True)
    #users_with_stimmberechtigung = get_final_dataframe_testing()

    update_main_report(users_with_stimmberechtigung, t_start)  # 1 edit
    append_current_value(t_start, users_with_stimmberechtigung)  # 1 edit
    accounts_by_registration_year(users_with_stimmberechtigung)  # 1 edits

    # TODO: currently unused; need to look at binning in order to have useful numbers
    #accounts_by_editcount(users_with_stimmberechtigung)  # 1 edit
    #accounts_by_editcount_ns0(users_with_stimmberechtigung)  # 1 edit
    #accounts_by_editcount_ns0_past_year(users_with_stimmberechtigung)  # 1 edit


if __name__=='__main__':
    main()
