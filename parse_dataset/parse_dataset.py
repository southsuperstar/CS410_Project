import pandas as pd
import json
import glob

bname = './dataset/yelp_academic_dataset_business.json'
# fdata = json.load(fname)
df = pd.read_json(bname, lines=True)
# df = df[df.city == 'Las Vegas']
df['city'] = df['city'].str.upper()
df = df.dropna(subset=['categories'])
df = df[df['categories'].str.contains('Restaurants')]
df = df[df['categories'].str.contains('Chinese')]
df = df[(df.review_count >= 100) & (df.is_open == 1) & (df.stars >= 3.5)]
dseries = df['business_id'].tolist()
# print(dseries)
# print(df)
df.to_csv('business_lv_chinese_restaurants.csv', encoding='utf-8')
# df = df.groupby('city')['name'].nunique().sort_values(ascending=True)
# print(df)

for i in range(6):
    uname = './dataset/yelp_academic_dataset_review0' + str(i)
    print(uname)
    review = pd.read_json(uname, lines=True)
    review = review.dropna()
    review = review[review['business_id'].isin(dseries)]
    print('write to csv:')
    review.to_csv('review_lv_chinese_restaurants_' + str(i) + '.csv', encoding='utf-8')

allFiles = glob.glob("./*review_*.csv")
frame = pd.DataFrame()
list_ = []
for file_ in allFiles:
    df = pd.read_csv(file_,index_col=None, header=0)
    list_.append(df)
frame = pd.concat(list_)
frame = frame.dropna()
frame.to_csv('review_lv_chinese_restaurants_all.csv', encoding='utf-8')

users = list(set(frame['user_id'].tolist()))
user_data = './yelp_academic_dataset_user.json'
user_df = pd.read_json(user_data, lines=True)
user_df = user_df.dropna()
user_df = user_df[user_df['user_id'].isin(users)]
user_df.to_csv('users_lv_chinese_restaurants.csv', encoding='utf-8')

results = review.merge(df, on='business_id')
results = results.merge(user_df, on='user_id')
results.to_csv('merged_user_review_business_results.csv', encoding='utf-8')