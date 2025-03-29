import streamlit as st
import duckdb
import gdown
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import plotly.graph_objects as go

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
)
select q.*, date, row_number() over (partition by team order by date asc) as round
from quarter_score_gamelog q
join cte_date c on q.gameid = c.gameid
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
    return close

@st.cache_data
def load_data_rnd():
    # Download the database file
    gdown.download(f"https://drive.google.com/uc?id={FILE_ID}", DB_FILE, quiet=False)
    con = duckdb.connect(DB_FILE)
    rnd = con.execute(sql).fetchdf()
    con.close()
    return rnd

away_pre = load_data_away()
home_pre = load_data_home()
rnd = load_data_rnd()

away = away_pre.merge(rnd, on = ['TEAM', 'GAMEID'], how = 'left')
home = home_pre.merge(rnd, on = ['TEAM', 'GAMEID'], how = 'left')

all_option = "All Rounds"
# Create options with the "All" option at the beginning
options = [all_option] + list(away["round"].unique())


season_select = st.sidebar.multiselect(
    "Select Round",
    options=options,
    default=[all_option]
)

if all_option in season_select:
    filtered_data_away = away
    filtered_data_home = home
else:
    filtered_data_away = away[away["round"].isin(season_select)]
    filtered_data_home = home[home["round"].isin(season_select)]


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
display_df = clean[['TEAM_self', 'off_EFG', 'off_TOV', 'off_OREB', 'off_FTR', 'def_EFG',
       'def_TOV', 'def_OREB', 'def_FTR']]

display_df.columns = ['Team Name', 'Off EFG%', 'Off TOV%', 'Off OREB%', 'Off FTR', 'Def EFG%', 'Def TOV%', 'Def OREB%', 'Def FTR']
st.dataframe(display_df)
