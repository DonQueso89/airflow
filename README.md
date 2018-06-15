# airflow

Tech Teaches Tech @ Mobpro 2018-06-15

## setup guide

cd ~   
git clone https://github.com/DonQueso89/airflow.git   
cd airflow   
mkvirtualenv airflow -a . --python=python3   
pip install -r requirements.txt   
airflow initdb   
airflow list_dags   
   
The last dag you see in the output should be 'winrate-from-druid'   
