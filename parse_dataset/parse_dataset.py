import pandas as pd
import glob


'''
    Parse the business json log from yelp dataset.
    Filter in the businesses fit below criteria:
        1. It is a restaurant.
        2. Is is a Chinese restaurant. (we use Chinese restaurant to prove the concept, tool can be expanded to other kinds of food as well.
        3. The review count of this business is at least 100. This is to avoid data being skewed due to limited reviews.
        4. The business is currently open.
    After filtering the data, convert to csv file.
'''

business_dataset_name = './dataset/yelp_academic_dataset_business.json'
business_df = pd.read_json(business_dataset_name, lines=True)
business_df['city'] = business_df['city'].str.upper()
business_df = business_df.dropna(subset=['categories'])
business_df = business_df[business_df['categories'].str.contains('Restaurants')]
business_df = business_df[business_df['categories'].str.contains('Chinese')]
business_df = business_df[(business_df.review_count >= 100) & (business_df.is_open == 1) & (business_df.stars >= 3.5)]
dseries = business_df['business_id'].tolist()
business_df.to_csv('business_chinese_restaurants.csv', encoding='utf-8')
# df = df.groupby('city')['name'].nunique().sort_values(ascending=True)


'''
    Parse the review json log from yelp dataset.
    Due to the size of the review json file, split it into 6 smaller files, and parse each file separately.
    Filter in only the reviews belong to above businesses.
    Merge filtered individual files together.
    Convert merged data to csv file.
'''

for i in range(6):
    review_dataset_name = './dataset/yelp_academic_dataset_review0' + str(i)
    review_df = pd.read_json(review_dataset_name, lines=True)
    review_df = review_df.dropna()
    review_df = review_df[review_df['business_id'].isin(dseries)]
    review_df.to_csv('review_chinese_restaurants_' + str(i) + '.csv', encoding='utf-8')

review_subfiles = glob.glob("./*review_*.csv")
all_review_df = pd.DataFrame()
list_ = []
for file_ in review_subfiles:
    df_ = pd.read_csv(file_, index_col=None, header=0)
    list_.append(df_)
all_review_df = pd.concat(list_)
all_review_df = all_review_df.dropna()
all_review_df.to_csv('review_chinese_restaurants_all.csv', encoding='utf-8')


'''
    Parse the user json log fom yelp dataset.
    Filter in only the users belong to above reviews.
    After filtering the data, convert to csv file.
'''

users = list(set(all_review_df['user_id'].tolist()))
user_dataset_name = './yelp_academic_dataset_user.json'
user_df = pd.read_json(user_dataset_name, lines=True)
user_df = user_df.dropna()
user_df = user_df[user_df['user_id'].isin(users)]
user_df.to_csv('users_chinese_restaurants.csv', encoding='utf-8')


'''
    Merge business, review, user three files together.
    Save the merged file to csv.
'''

results = review_df.merge(business_df, on='business_id')
results = results.merge(user_df, on='user_id')
results.to_csv('merged_user_review_business_results.csv', encoding='utf-8')