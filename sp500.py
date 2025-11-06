import pandas as pd
import requests
link = ("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies#S&P_500_component_stocks")
headers = {"User-Agent":"Chrome/120.0.0.0 wikidatasource/1.0 (full email here) reason for pulling data here"} #use your own email
response = requests.get(link, headers=headers)
in_sp = pd.read_html(response.text)
sp1 = in_sp[0] #table 1 of current members
ch_sp = in_sp[1] #table 2 for changes
sp2 = sp1[['Symbol']]
sp2['Date'] = pd.Timestamp.today().strftime('%Y-%m-%d') #replace with today's date
sp2.columns = ['ticker', 'Date']
sp2['Date'] = pd.to_datetime(sp2['Date'])
sp3 = sp2.dropna()
sp_adds = ch_sp.T.reset_index().iloc[0:2, 2:].T #isolate additions
sp_adds.columns = ['Date', 'ticker']
sp_adds['Date'] = pd.to_datetime(sp_adds['Date'])
sp_adds2 = sp_adds.dropna()
sp_removed = ch_sp.T.reset_index().iloc[[0,3], 2:].T #isolate removals
sp_removed.columns = ['Date', 'ticker']
sp_removed['Date'] = pd.to_datetime(sp_removed['Date'])
sp_removed2 = sp_removed.dropna()
unique_dates = sp_adds2['Date'].unique() #get list of unique dates
full_dates = pd.date_range(start="1976-07-01", end=pd.Timestamp.today().strftime('%Y-%m-%d'), freq="D") #if want to use quarter, convert s&p dates to nearest/next quarter
event_dates = sorted(unique_dates, reverse=True)
current_members = set(sp3['ticker'])
sp_dict = {} #store all in dictionaries
for d in event_dates: #loop goes in reverse so remove additions and add removed, loop only stores on days changes made
    sp_dict[d] = pd.DataFrame({'Date': d, 'ticker': list(current_members)})
    adds = sp_adds2.loc[sp_adds2['Date'] == d, 'ticker']
    removes = sp_removed2.loc[sp_removed2['Date'] == d, 'ticker']
    current_members.difference_update(adds) 
    current_members.update(removes)
sp_event_panel = pd.concat(sp_dict.values(), ignore_index=True) #concat all
sp_event_panel['Date'] = pd.to_datetime(sp_event_panel['Date'])
sp_event_panel = sp_event_panel.sort_values('Date')
event_sorted = sorted(sp_event_panel['Date'].unique())
daily_records = []
current_set = set()
for d in full_dates: #make it daily panel data
    while event_sorted and d >= event_sorted[0]:
        current_set = set(sp_event_panel.loc[sp_event_panel['Date'] == event_sorted[0], 'ticker'])
        event_sorted.pop(0)
    if current_set:
        for t in current_set:
            daily_records.append({'Date': d, 'ticker': t})
sp_daily_panel = pd.DataFrame(daily_records) #to pandas dataframe
sp_daily_panel.to_csv('sp500_hist.csv') #write to csv