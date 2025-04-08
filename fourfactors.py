import streamlit as st
import duckdb
import gdown
import pandas as pd

# Set page configuration as the first Streamlit call
st.set_page_config(layout="wide")

DB_FILE = "nbl.db"
FILE_ID = st.secrets["FILE_ID"]
sql_away = """
select * from away_player_game_log
"""
sql_home = """
select * from home_player_game_log
"""
sql = """
with cte_date as (
select q.gameid, date
from quarter_score_gamelog q
join away_player_game_log a on q.gameid = a.gameid
group by 1,2
),
rnd as (
select q.*, date, row_number() over (partition by team order by date asc) as round
from quarter_score_gamelog q
join cte_date c on q.gameid = c.gameid
),
opp as (
select q.gameid, q.team, q2.team as opp_team
from quarter_score_gamelog q
join quarter_score_gamelog q2 on q.gameid = q2.gameid and q.team <> q2.team
)
select r.*, opp_team
from rnd r
join opp o on r.gameid = o.gameid and r.team = o.team
"""
sql_gamelog = """
with cte_ff as (
select ff.*, ff2.efg as opp_efg, ff2.tov as opp_tov, ff2.oreb as opp_oreb, ff2.ftr as opp_ftr
from four_factor_gamelog ff
join four_factor_gamelog ff2 on ff.gameid = ff2.gameid and ff.team <> ff2.team
)
select ff.*, poss, ortg, drtg, netrtg, nrg.date as date_update
from cte_ff ff
join net_rating_gamelog nrg on ff.team = nrg.team and ff.gameid = nrg.gameid
"""
sql_adv = """
select *
from player_advanced
"""
sql_trad = """
select *
from player_traditional
"""


@st.cache_data
def load_data_away():
    # Download the database file
    gdown.download(f"https://drive.google.com/uc?id={FILE_ID}", DB_FILE, quiet=False)
    con = duckdb.connect(DB_FILE)
    away_pre = con.execute(sql_away).fetchdf()
    con.close()
    return away_pre

@st.cache_data
def load_data_home():
    # Download the database file
    gdown.download(f"https://drive.google.com/uc?id={FILE_ID}", DB_FILE, quiet=False)
    con = duckdb.connect(DB_FILE)
    home_pre = con.execute(sql_home).fetchdf()
    con.close()
    return home_pre

@st.cache_data
def load_data_rnd():
    # Download the database file
    gdown.download(f"https://drive.google.com/uc?id={FILE_ID}", DB_FILE, quiet=False)
    con = duckdb.connect(DB_FILE)
    rnd = con.execute(sql).fetchdf()
    con.close()
    return rnd

@st.cache_data
def load_data_gamelog():
    # Download the database file
    gdown.download(f"https://drive.google.com/uc?id={FILE_ID}", DB_FILE, quiet=False)
    con = duckdb.connect(DB_FILE)
    gl = con.execute(sql_gamelog).fetchdf()
    con.close()
    return gl
@st.cache_data
def load_data_adv():
    # Download the database file
    gdown.download(f"https://drive.google.com/uc?id={FILE_ID}", DB_FILE, quiet=False)
    con = duckdb.connect(DB_FILE)
    adv = con.execute(sql_adv).fetchdf()
    con.close()
    return adv
@st.cache_data
def load_data_trad():
    # Download the database file
    gdown.download(f"https://drive.google.com/uc?id={FILE_ID}", DB_FILE, quiet=False)
    con = duckdb.connect(DB_FILE)
    trad = con.execute(sql_trad).fetchdf()
    con.close()
    return trad


away_pre = load_data_away()
home_pre = load_data_home()
rnd = load_data_rnd()
gamelog_pre = load_data_gamelog()
adv = load_data_adv()
trad = load_data_trad()

away = away_pre.merge(rnd, on = ['TEAM', 'GAMEID'], how = 'left')
home = home_pre.merge(rnd, on = ['TEAM', 'GAMEID'], how = 'left')


away['DATE_format'] = away['DATE_x'].map(lambda x: pd.Timestamp(x))
home['DATE_format'] = home['DATE_x'].map(lambda x: pd.Timestamp(x))

start_date = away['DATE_format'].min()
end_date = away['DATE_format'].max()

start_date = start_date.to_pydatetime()
end_date = end_date.to_pydatetime()


today_display = gamelog_pre['date_update'].max()

st.header('League Four Factors Percentile Rank')
st.write('Last updated: ' + str(today_display))

selected_dates = st.slider(
    "Select date range",
    min_value=start_date,
    max_value=end_date,
    value=(start_date, end_date),
    format="YYYY-MM-DD"
)

string_list = [dt.strftime('%Y-%m-%d') for dt in selected_dates]

filtered_data_home = home[(home['DATE_x'] >= string_list[0]) & (home['DATE_x'] <= string_list[1])]
filtered_data_away = away[(away['DATE_x'] >= string_list[0]) & (away['DATE_x'] <= string_list[1])]



home_agg = filtered_data_home.groupby(['GAMEID', 'TEAM']).agg({
    'DEF':'sum',
    'OFF':'sum',
    'FGM':'sum',
    'FGA':'sum',
    '3FGM':'sum',
    'TOV':'sum',
    'FTA':'sum',
    'FTM':'sum'
}).reset_index()

away_agg = filtered_data_away.groupby(['GAMEID', 'TEAM']).agg({
    'DEF':'sum',
    'OFF':'sum',
    'FGM':'sum',
    'FGA':'sum',
    '3FGM':'sum',
    'TOV':'sum',
    'FTA':'sum',
    'FTM':'sum'
}).reset_index()

home_tot = away_agg.merge(home_agg, on = ['GAMEID'], how = 'left', suffixes = ('_self', '_opp'))
away_tot = home_agg.merge(away_agg, on = ['GAMEID'], how = 'left', suffixes = ('_self', '_opp'))

tot = pd.concat([home_tot, away_tot])
clean = tot.groupby(['TEAM_self']).sum().reset_index().drop(columns = 'GAMEID')

clean['off_EFG'] = (clean['FGM_self'] + .5*clean['3FGM_self'])/clean['FGA_self']
clean['off_TOV'] = clean['TOV_self']/(clean['FGA_self'] + clean['FTA_self']*.44 + clean['TOV_self'])
clean['off_OREB'] = clean['OFF_self']/(clean['OFF_self'] + clean['DEF_opp'])
clean['off_FTR'] = clean['FTM_self']/clean['FGA_self']

clean['def_EFG'] = (clean['FGM_opp'] + .5*clean['3FGM_opp'])/clean['FGA_opp']
clean['def_TOV'] = clean['TOV_opp']/(clean['FGA_opp'] + clean['FTA_opp']*.44 + clean['TOV_opp'])
clean['def_OREB'] = clean['OFF_opp']/(clean['OFF_opp'] + clean['DEF_self'])
clean['def_FTR'] = clean['FTM_opp']/clean['FGA_opp']


clean['off_EFG_rank'] = clean['off_EFG'].rank(pct=True)
clean['off_TOV_rank'] = 1-clean['off_TOV'].rank(pct=True)
clean['off_OREB_rank'] = clean['off_OREB'].rank(pct=True)
clean['off_FTR_rank'] = clean['off_FTR'].rank(pct=True)


clean['def_EFG_rank'] = 1-clean['def_EFG'].rank(pct=True)
clean['def_TOV_rank'] = clean['def_TOV'].rank(pct=True)
clean['def_OREB_rank'] = 1-clean['def_OREB'].rank(pct=True)
clean['def_FTR_rank'] = 1-clean['def_FTR'].rank(pct=True)



display_df = clean[['TEAM_self', 'off_EFG_rank', 'off_TOV_rank', 'off_OREB_rank', 'off_FTR_rank', 'def_EFG_rank',
       'def_TOV_rank', 'def_OREB_rank', 'def_FTR_rank']]

display_df.iloc[:, 1:] = display_df.iloc[:, 1:].round(2)
display_df.columns = ['Team Name', 'Off EFG% Rank', 'Off TOV% Rank', 'Off OREB% Rank', 'Off FTR Rank', 'Def EFG% Rank', 'Def TOV% Rank', 'Def OREB% Rank', 'Def FTR Rank']
styled_df = display_df.style.background_gradient(cmap='coolwarm', subset=['Off EFG% Rank', 'Off TOV% Rank', 'Off OREB% Rank', 'Off FTR Rank', 'Def EFG% Rank', 'Def TOV% Rank', 'Def OREB% Rank', 'Def FTR Rank'])
styled_df = styled_df.format({col: '{:.2f}' for col in display_df.columns[1:]})

st.dataframe(styled_df, height=460)

gamelog = gamelog_pre.merge(rnd[['GAMEID', 'DATE', 'TEAM', 'opp_team']].drop_duplicates(), on = ['TEAM', 'GAMEID'], how = 'left')

all_option = "All Teams"
# Create options with the "All" option at the beginning
options = [all_option] + list(gamelog["TEAM"].unique())

# Multiselect widget with default as "All Seasons"
season_select = st.multiselect(
    "Select Team",
    options=options,
    default=[all_option]
)

# If "All Seasons" is selected, ignore filtering
if all_option in season_select:
    filtered_data = gamelog
    filtered_data_adv = adv
    filtered_data_trad = trad
else:
    filtered_data = gamelog[gamelog["TEAM"].isin(season_select)]
    filtered_data_adv = adv[adv["TEAM"].isin(season_select)]
    filtered_data_trad = trad[trad["TEAM"].isin(season_select)]

ff = filtered_data[['TEAM', 'opp_team', 'DATE', 'EFG', 'TOV', 'OREB', 'FTR', 'opp_efg', 'opp_tov', 'opp_oreb', 'opp_ftr']]
ff.sort_values('DATE', ascending = True, inplace = True)
league_avg = list(gamelog[['EFG', 'TOV', 'OREB', 'FTR', 'opp_efg', 'opp_tov',
       'opp_oreb', 'opp_ftr']].mean())
league_avg.insert(0, '')
league_avg.insert(0, '')
league_avg.insert(0, 'League Average')

ff.loc[len(ff) + 1] = league_avg

ff.columns = ['Team', 'Opp Team', 'Date', 'Off EFG%', 'Off TOV%', 'Off OREB%', 'Off FTR', 'Def EFG%', 'Def TOV%', 'Def OREB%', 'Def FTR']
nr = filtered_data[['TEAM', 'opp_team', 'DATE', 'POSS', 'ORTG', 'DRTG', 'NETRTG']]
nr.columns = ['Team', 'Opp Team', 'Date', 'Possessions', 'OffRtg', 'DefRtg', 'NetRtg']


st.header('Game Logs')

tab = st.radio("", ["Four Factors", "Net Rating"])
styled_df_ff = ff.style


# Tab 1 content
if tab == "Four Factors":
    st.header('Four Factors Game Log')
    styled_df_ff = styled_df_ff.format({col: '{:.2f}' for col in ff.columns[3:]})
    st.dataframe(styled_df_ff)


# Tab 2 content
if tab == "Net Rating":
    st.header('Net Ratings Game Log')
    st.dataframe(nr)


tab2 = st.radio("", ["Advanced", "Traditional"])

filtered_data_adv.columns = ['Player', 'Team', 'TS%', 'eFG%', 'ORB%', 'DRB%',
       'TRB%', 'AST%', 'TOV%', 'STL%', 'BLK%', 'USG%', 'ORtg',
       'DRtg', 'eDiff', 'season']
filtered_data_trad.columns = ['Player', 'Team', 'GP', 'MPG', 'PPG', 'FGM', 'FGA', 'FG%', '3PM',
       '3PA', '3P%', 'FTM', 'FTA', 'FT%', 'ORB', 'DRB', 'RPG', 'APG', 'SPG',
       'BPG', 'TOV', 'season']
# Tab 1 content
if tab2 == "Advanced":
    st.header('Player Box Score - Advanced')
    st.dataframe(filtered_data_adv[filtered_data_adv['season'] == '2025'].sort_values('USG%', ascending = False))

# Tab 2 content
if tab2 == "Traditional":
    st.header('Player Box Score - Traditional')
    st.dataframe(filtered_data_trad[filtered_data_trad['season'] == '2025'].sort_values('PPG', ascending = False))

options = filtered_data_trad[filtered_data_trad['season'] == '2025']['Player'].unique()

# Create the selectbox widget
selected_option = st.selectbox('Select Player', options)

# Tab 1 content
if tab2 == "Advanced":
    st.header('Player Y/Y - Advanced')
    st.dataframe(filtered_data_adv[filtered_data_adv['Player'] == selected_option].sort_values('season', ascending = True))

# Tab 2 content
if tab2 == "Traditional":
    st.header('Player Y/Y - Traditional')
    st.dataframe(filtered_data_trad[filtered_data_trad['Player'] == selected_option].sort_values('season', ascending = True))
