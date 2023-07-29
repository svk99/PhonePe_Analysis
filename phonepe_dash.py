import pandas as pd
import streamlit as st
import mysql.connector
import plotly.express as px

db = mysql.connector.connect(
    host='localhost',
    user='*****',
    password='*****',
    database='*****'
)
mycursor = db.cursor()


st.header('Transaction analysis')
t1, t2 = st.tabs(['State analysis', 'District analysis'])
with t1:
    df_agg_transact = pd.read_csv('.../Agg_transaction.csv', index_col=0)
    transact = df_agg_transact.copy()
    col1, col2, col3 = st.columns(3)
    with col1:
        type = st.selectbox('Select payment type', ('Recharge & bill payments', 'Peer-to-peer payments',
                                                    'Merchant payments', 'Financial Services', 'Others'))
    with col2:
        state = st.selectbox('Select State', ('andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh',
                                              'assam', 'bihar', 'chandigarh', 'chhattisgarh',
                                              'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat',
                                              'haryana', 'himachal-pradesh', 'jammu-&-kashmir',
                                              'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep',
                                              'madhya-pradesh', 'maharashtra', 'manipur', 'meghalaya', 'mizoram',
                                              'nagaland', 'odisha', 'puducherry', 'punjab', 'rajasthan',
                                              'sikkim', 'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh',
                                              'uttarakhand', 'west-bengal'), key='a')
    with col3:
        tran = st.selectbox('Select transaction parameter', ('Transaction count', 'Transaction amount'))
    State = state
    Year_list = [2018, 2019, 2020, 2021, 2022]
    Payment_type = type
    transact = transact.loc[(transact['State'] == State) & (transact['Year'].isin(Year_list)) &
                            (transact['Transaction_type'] == Payment_type)]
    transact['Quarter'] = 'Q' + transact['Quarter'].astype(str)
    transact['Year_Quarter'] = transact['Year'].astype(str) + '-' + transact['Quarter'].astype(str)
    transact['Transaction_amount'] = transact['Transaction_amount'].astype('int64')
    if tran == 'Transaction count':
        fig = px.bar(transact, x='Year_Quarter', y='Transaction_count', color='Transaction_count',
                     color_continuous_scale='Viridis')
        st.write('#### ' + State.upper())
        st.plotly_chart(fig)
    elif tran == 'Transaction amount':
        fig = px.bar(transact, x='Year_Quarter', y='Transaction_amount', color='Transaction_amount',
                     color_continuous_scale='Viridis')
        st.write('#### ' + State.upper())
        st.plotly_chart(fig)

with t2:
    c1, c2, c3 = st.columns(3)
    with c1:
        year = st.selectbox('Select year', ('2018', '2019', '2020', '2021', '2022'), key='b')
    with c2:
        state = st.selectbox('Select State', ('andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh',
                                              'assam', 'bihar', 'chandigarh', 'chhattisgarh',
                                              'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat',
                                              'haryana', 'himachal-pradesh', 'jammu-&-kashmir',
                                              'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep',
                                              'madhya-pradesh', 'maharashtra', 'manipur', 'meghalaya', 'mizoram',
                                              'nagaland', 'odisha', 'puducherry', 'punjab', 'rajasthan',
                                              'sikkim', 'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh',
                                              'uttarakhand', 'west-bengal'), key='c')
    with c3:
        qtr = st.selectbox('Select Quarter', ('1', '2', '3', '4'), key='d')
    c_a = st.radio('Select', ('Count', 'Amount'), horizontal=True)
    Year = int(year)
    Quart = int(qtr)
    map_transact = pd.read_csv('.../map_transaction.csv')
    map_transact['Amount'] = map_transact['Amount'].astype('int64')
    district = map_transact.loc[(map_transact['State'] == state) & (map_transact['Year'] == Year) &
                                (map_transact['Quarter'] == Quart)]
    l = len(district)
    if c_a == 'Count':
        fig = px.bar(district, x='District', y='Count', color='Count', color_continuous_scale='Viridis')
        st.write('#### ' + state.upper() + ' WITH ' + str(l) + ' DISTRICTS')
        st.plotly_chart(fig)
    elif c_a == 'Amount':
        fig = px.bar(district, x='District', y='Amount', color='Amount', color_continuous_scale='Viridis')
        st.write('#### ' + state.upper() + ' WITH ' + str(l) + ' DISTRICTS')
        st.plotly_chart(fig)

st.header('User analysis')
tab1, tab2 = st.tabs(['State analysis', 'District analysis'])
with tab1:
    df_agg_user = pd.read_csv('.../Agg_user.csv', index_col=0)
    user = df_agg_user.copy()
    col1, col2 = st.columns(2, gap='medium')
    with col1:
        year = st.selectbox('Select year', ('2018', '2019', '2020', '2021', '2022'), key='e')
    with col2:
        state = st.selectbox('Select State', ('andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh',
                                              'assam', 'bihar', 'chandigarh', 'chhattisgarh',
                                              'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat',
                                              'haryana', 'himachal-pradesh', 'jammu-&-kashmir',
                                              'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep',
                                              'madhya-pradesh', 'maharashtra', 'manipur', 'meghalaya', 'mizoram',
                                              'nagaland', 'odisha', 'puducherry', 'punjab', 'rajasthan',
                                              'sikkim', 'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh',
                                              'uttarakhand', 'west-bengal'), key='f')
    Year = int(year)
    State = state
    sum_by_group = user.groupby(['State', 'Year', 'Brand'])['Count'].sum()
    df_sum = sum_by_group.reset_index()
    df_sum = df_sum.loc[(df_sum['State'] == State) & (df_sum['Year'] == Year)]
    fig = px.pie(df_sum, values='Count', names='Brand', color_discrete_sequence=px.colors.sequential.Aggrnyl)
    st.plotly_chart(fig)
with tab2:
    c1, c2, c3 = st.columns(3)
    with c1:
        year = st.selectbox('Select year', ('2018', '2019', '2020', '2021', '2022'), key='g')
    with c2:
        state = st.selectbox('Select State', ('andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh',
                                              'assam', 'bihar', 'chandigarh', 'chhattisgarh',
                                              'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat',
                                              'haryana', 'himachal-pradesh', 'jammu-&-kashmir',
                                              'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep',
                                              'madhya-pradesh', 'maharashtra', 'manipur', 'meghalaya', 'mizoram',
                                              'nagaland', 'odisha', 'puducherry', 'punjab', 'rajasthan',
                                              'sikkim', 'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh',
                                              'uttarakhand', 'west-bengal'), key='h')
    with c3:
        qtr = st.selectbox('Select Quarter', ('1', '2', '3', '4'), key='i')
    Year = int(year)
    Quart = int(qtr)
    map_user = pd.read_csv('.../map_user.csv')
    district = map_user.loc[(map_user['State'] == state) & (map_user['Year'] == Year) &
                            (map_user['Quarter'] == Quart)]
    fig = px.pie(district, values='Registered_users', names='District',
                 color_discrete_sequence=px.colors.sequential.Aggrnyl)
    st.plotly_chart(fig)

st.header('Geo visualization')
y = st.selectbox('Select year', ('2018', '2019', '2020', '2021', '2022'),key='j')
q = st.selectbox('Select Quarter', ('1', '2', '3', '4'),key='k')
t = st.radio('Select', ('Transaction Count', 'Transaction Amount'))
Year = int(y)
Quarter = int(q)
mycursor.execute(
    f"select state, sum(count) as Total_Transaction_count, sum(amount) as Total_Transaction_amount from map_transact where year = {Year} and quarter = {Quarter} group by state order by state ")
df1 = pd.DataFrame(mycursor.fetchall(), columns=['state', 'Total_Transaction_count', 'Total_Transaction_amount'])
df2 = pd.read_csv(".../Statenames.csv")
df1.state = df2
if t == 'Transaction Amount':
    fig1 = px.choropleth(df1,
                         geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                         featureidkey='properties.ST_NM',
                         locations='state',
                         color='Total_Transaction_amount',
                         color_continuous_scale='Aggrnyl',
                         fitbounds='locations')
    fig1.update_geos(visible=False)
    st.plotly_chart(fig1, use_container_width=True)
elif t == 'Transaction Count':
    fig1 = px.choropleth(df1,
                         geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                         featureidkey='properties.ST_NM',
                         locations='state',
                         color='Total_Transaction_count',
                         color_continuous_scale='Aggrnyl',
                         fitbounds='locations')
    fig1.update_geos(visible=False)
    st.plotly_chart(fig1, use_container_width=True)


st.subheader('Select the question:')
question=st.selectbox('Questions',
               ['Click the question that you would like to query',
    '1. Which top 10 states did most recharge and bill payments in 2020?',
    '2. Which top 10 states did most peer to peer payments in 2022?',
    '3. Which top 10 districts did most no of transaction in 2021?',
    '4. Which top 10 districts did most highest amount of transaction in 2019?'
               ])

if question == '1. Which top 10 states did most recharge and bill payments in 2020?':
    mycursor.execute(
        """select state as State,sum(tran_count) as Count from agg_transact where year=2020 and tran_type='Recharge & bill payments' group by state order by Count desc limit 10""")
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write(df)
    fig = px.bar(df, x='State', y='Count', color='Count', color_continuous_scale='Viridis')
    st.plotly_chart(fig)

if question == '2. Which top 10 states did most peer to peer payments in 2022?':
    mycursor.execute(
        """select state as State,sum(tran_count) as Count from agg_transact where year=2022 and tran_type='Peer-to-peer payments' group by state order by Count desc limit 10""")
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write(df)
    fig = px.bar(df, x='State', y='Count', color='Count', color_continuous_scale='Viridis')
    st.plotly_chart(fig)

if question == '3. Which top 10 districts did most no of transaction in 2021?':
    mycursor.execute(
        """select district as District,sum(count) as Count from map_transact where year = 2021 group by district order by Count desc limit 10""")
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write(df)
    fig = px.bar(df, x='District', y='Count', color='Count', color_continuous_scale='Viridis')
    st.plotly_chart(fig)

if question == '4. Which top 10 districts did most highest amount of transaction in 2019?':
    mycursor.execute(
        """select district as District,sum(truncate(amount,0)) as Amount from map_transact where year = 2019 group by district order by Amount desc limit 10;""")
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write(df)
    fig = px.bar(df, x='District', y='Amount', color='Amount', color_continuous_scale='Viridis')
    st.plotly_chart(fig)
