from google.cloud import storage
from urllib import request


DATASETS = [
    'Wireless_v1_00',
    'Watches_v1_00',
    'Video_Games_v1_00',
    'Video_DVD_v1_00',
    'Video_v1_00',
    'Toys_v1_00',
    'Tools_v1_00',
    'Sports_v1_00',
    'Software_v1_00',
    'Shoes_v1_00',
    'Pet_Products_v1_00',
    'Personal_Care_Appliances_v1_00',
    'PC_v1_00',
    'Outdoors_v1_00',
    'Office_Products_v1_00',
    'Musical_Instruments_v1_00',
    'Music_v1_00',
    'Mobile_Electronics_v1_00',
    'Mobile_Apps_v1_00',
    'Major_Appliances_v1_00',
    'Luggage_v1_00',
    'Lawn_and_Garden_v1_00',
    'Kitchen_v1_00',
    'Jewelry_v1_00',
    'Home_Improvement_v1_00',
    'Home_Entertainment_v1_00',
    'Home_v1_00',
    'Health_Personal_Care_v1_00',
    'Grocery_v1_00',
    'Gift_Card_v1_00',
    'Furniture_v1_00',
    'Electronics_v1_00',
    'Digital_Video_Games_v1_00',
    'Digital_Video_Download_v1_00',
    'Digital_Software_v1_00',
    'Digital_Music_Purchase_v1_00',
    'Digital_Ebook_Purchase_v1_00',
    'Camera_v1_00',
    'Books_v1_00',
    'Beauty_v1_00',
    'Baby_v1_00',
    'Automotive_v1_00',
    'Apparel_v1_00',
    'Digital_Ebook_Purchase_v1_01',
    'Books_v1_01',
    'Books_v1_02',
]  # see https://github.com/huggingface/datasets/blob/master/datasets/amazon_us_reviews/amazon_us_reviews.py
DATA_URL = 'https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_{}.tsv.gz'
GCS_BUCKET = '[BUCKET_NAME]'
GCS_PATH = 'datasets/amazon-us-reviews/raw'

client = storage.Client()
bucket = client.get_bucket(GCS_BUCKET)


if __name__ == '__main__':
    for file in DATASETS:
        print(f'Requesting {file}...')
        response = request.urlopen(DATA_URL.format(file))
        if response.status == 200:
            blob = bucket.blob(f'{GCS_PATH}/{file}.tsv.gz')
            blob.content_type = 'text/tab-separated-values'
            blob.content_encoding = 'gzip'
            print(f'Uploading to gs://{GCS_BUCKET}/{GCS_PATH}/{file}.tsv.gz...')
            blob.upload_from_string(response.fp.read(), content_type=blob.content_type)
        else:
            print('An error occurred.')
