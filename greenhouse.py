import requests
import pandas as pd
from datetime import date, timedelta, datetime
from discord_webhook import DiscordWebhook, DiscordEmbed
import time
import json
import mysql.connector
import timeit

#Input your Discord credentials to connect to your channel webhook.
#Note - Multiple webhooks can be inserted, just copy and paste the original value below.
groups = {
    'INSERT BOT NAME': {
        'name': 'INSERT BOT NAME',
        'color': 'INSERT HEX COLOR',
        'webhook': 'INSERT DISCORD WEBHOOK',
        'filtered_webhook': 'INSERT DISCORD WEBHOOK'},
}

#This section connects your database source. For my use case, mySQL was the selection of choice.

#connection created
mydb = mysql.connector.connect(
  host="INSERT HOST NAME",
  user="INSERT USERNAME",
  password="INSERT PASSWORD",
  database="INSERT DATABASE NAME"
)

mycursor = mydb.cursor()

#Insert any front end greenhouse job listing.
url_link = [
    'https://boards.greenhouse.io/goatgroup'
    , 'https://boards.greenhouse.io/tusimple'
    , 'https://boards.greenhouse.io/poshmark'
    , 'https://boards.greenhouse.io/distantjob'
    , 'https://boards.greenhouse.io/aptoslabs'
    , 'https://boards.greenhouse.io/avantstay'
    , 'https://boards.greenhouse.io/tubitv'
    , 'https://boards.greenhouse.io/stockx'
    ]

#Insert any backend greenhouse job listing.
api_link = [
    'https://boards-api.greenhouse.io/v1/boards/lyft/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/bigpanda/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/survata/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/pieces/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/doordash/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/discord/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/fanduel/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/datadog/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/stripe/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/10xgenomics/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/whatnot/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/pokemoncareers/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/2k/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/duolingo/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/chief/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/nuna/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/notion/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/buzzfeed/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/fastly/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/cityoffortworth/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/nationalpublicradioinc/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/clifbar15/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/affirm/jobs/'
    , 'https://boards-api.greenhouse.io/v1/boards/upwork/jobs'
    ]

#Checks any new job postings.
def main(url_link):
    #Insert User Agent Credentials.
    headers = {
    "Connection": "keep-alive",
    "User-Agent": "INSERT USER AGENT"
    }
    
    main_dict = []
    
    #Checks the response is valid.
    response = requests.get(url_link, headers=headers)
    if response.status_code == 200:
        
        #Load Json Data
        data = json.loads(response.text)
        data = data['jobs']
        
        #Captures all Json Data
        for i in data:
            job_link_path = i['absolute_url']
            
            #Checks new job url link exists in current database.
            if job_link_path not in url_list:
                
                #Captures job title, location, company, and updated at timestamp.
                job_title = i['title']
                job_location = i['location']['name']

                job_id = i['id']

                if job_link_path.split('/')[2] != 'boards.greenhouse.io':
                    company = url_link.split('/')[-3]
                else:
                    company = job_link_path.split('/')[3]

                updated_at = i['updated_at']
                updated_at_object = datetime.strptime(updated_at, "%Y-%m-%dT%H:%M:%S%z")
                
                #Captures job specific api for each job id
                job_id_url = 'https://boards-api.greenhouse.io/v1/boards/{}/jobs/{}'.format(company, job_id)

                #Captures the job location and department.
                response2 = requests.get(job_id_url)
                if response2.status_code == 200:
                    data2 = json.loads(response2.text)
                    if data2['offices'] != []:
                        job_locaton_clean = data2['offices']
                        for i2 in job_locaton_clean:
                            job_location_clean = i2['location']
                            if job_location_clean == None:
                                job_location_clean = job_location
                    else:
                        job_location_clean = ''


                    department = data2['departments']
                    # print(department)
                    if department == []:
                        job_department = ''
                    else:
                        for d in department:
                            job_department = d['name']

                    #Transform all captured data into a dictionary.
                    data_dict = {
                    'job_title': job_title,
                    'job_link_path': job_link_path,
                    'job_location': job_location_clean,
                    'job_department': job_department,
                    'job_id': job_id,
                    'company': company,
                    'created_at': datetime.now(),
                    'updated_at': updated_at_object,
                    'job_workplace_type': '',
                    'job_board': 'greenhouse',
                    }

                    main_dict.append(data_dict)

                    #Inserts all the captured data into the mySQL database.
                    sql_insert = "INSERT INTO jobs (job_title, job_link_path, job_location, job_department, job_id, workplace_type, company, created_at, updated_at, job_board) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    sql_values = (data_dict['job_title'], data_dict['job_link_path'], data_dict['job_location'], data_dict['job_department'], data_dict['job_id'], data_dict['job_workplace_type'], data_dict['company'], data_dict['created_at'], data_dict['updated_at'], data_dict['job_board'])
                    mycursor.execute(sql_insert, sql_values)

                    #Transforms the dictionary into a pandas dataframe.
                    data_df = pd.DataFrame(data_dict, index=[0])

                    #Iterates through the pandas dataframe and discord webhooks.
                    for i in range(0, len(data_df)):
                        for z in groups.values():
                        
                            #Takes each Discord object and inserts into the following parameters.
                            name = z['name']
                            color = 'AFE1AF'
                            webhook = z['webhook']
                            filtered_webhook = z['filtered_webhook']

                            webhook_name = DiscordWebhook(url=webhook, username=name)

                            embed = DiscordEmbed(title="New Job Posted from {}!".format(data_df['company'][i]),description="Title of the role is **{}.**".format(data_df['job_title'][i]),color=color)

                            embed.add_embed_field(name="Company",value=data_df['company'][i],inline=True)
                            embed.add_embed_field(name="Job Department",value=data_df['job_department'][i],inline=True)
                            embed.add_embed_field(name="Job Location",value=data_df['job_location'][i],inline=True)
                            embed.add_embed_field(name="Job ID",value=int(data_df['job_id'][i]),inline=True)
                            embed.add_embed_field(name="Job Link",value='[Job Posting Link]({})'.format(data_df['job_link_path'][i]),inline=True)
                            embed.set_timestamp()
                            webhook_name.add_embed(embed)
                            
                            #Webhook is sent of the new job posting.
                            response = webhook_name.execute(remove_embeds=True)
                            
                            #Sleeping to prevent Discord rate limit of 50 messages every 15 second interval.
                            time.sleep(1)

                            #Insert any keywords that match a job listing to post into a different channel.
                            keyword = ['Marketing', 'Marketing Associate', 'Marketing Coordinator', 'Marketing Manager', 'Brand', 'Copywriting', 'Copywrite', 'Analyst', 'Senior Analyst', 'Data Analyst', 'Senior Data Analyst', 'Senior Business Analyst']
                            job_title_lower = data_df['job_title'].str
                            result = job_title_lower.contains('|'.join(keyword))
                            
                            if any(result):
                                #Takes each Discord object and inserts into the following parameters for keywords only.

                                webhook_name2 = DiscordWebhook(url=filtered_webhook, username=name)

                                embed2 = DiscordEmbed(title="New Job Posted from {}!".format(data_df['company'][i]),description="Title of the role is **{}.**".format(data_df['job_title'][i]),color=color)

                                embed2.add_embed_field(name="Job Title",value=data_df['job_title'][i],inline=False)
                                embed2.add_embed_field(name="Company",value=data_df['company'][i],inline=True)
                                embed2.add_embed_field(name="Job Department",value=data_df['job_department'][i],inline=True)
                                embed2.add_embed_field(name="Job Location",value=data_df['job_location'][i],inline=True)
                                embed2.add_embed_field(name="Job ID",value=int(data_df['job_id'][i]),inline=True)
                                embed2.add_embed_field(name="Job Link",value='[Job Posting Link]({})'.format(data_df['job_link_path'][i]),inline=True)
                                embed2.set_timestamp()
                                webhook_name2.add_embed(embed)
                                
                                #Webhook is sent of the new job posting.
                                response2 = webhook_name2.execute(remove_embeds=True)
                                
                                #Sleeping to prevent Discord rate limit of 50 messages every 15 second interval.
                                time.sleep(1)
                                
                            else:
                                pass
            else:
                pass
    else:
        pass

#This function checks any job listings that were removed from a company.
def check_removed_urls():
    deleted_at_url_list = []
    
    #Iterates through current database urls.
    for urls in url_list:
        job_id_query = urls.split('/')[-1]
        job_company_query = urls.split('/')[-3]
        job_id_url_query = 'https://boards-api.greenhouse.io/v1/boards/{}/jobs/{}'.format(job_company_query, job_id_query)
        response2 = requests.get(job_id_url_query)
        
        #Checking if any current urls have a 404 response. If a 404 response is present, update the mySQL field called 'deleted_at' with the current timestamp.
        if urls not in deleted_at_url_list:
            if response2.status_code == 404:
                if query_dict[urls]['deleted_at'] is None:

                    deleted_at_dict = {
                        'job_link_path': urls,
                        'deleted_at': date.today(),
                        'job_title': query_dict[urls]['job_title'],
                        'company': query_dict[urls]['company'],
                        'job_location': query_dict[urls]['location'],
                        }
                        
                    deleted_at_url_list.append(urls)
  
                    #Updated deleted_at field with current timestamp.
                    deleted_at_df = pd.DataFrame(deleted_at_dict, index=[0])
                    sql = "UPDATE jobs SET deleted_at = %s WHERE job_link_path = %s"
                    val = (deleted_at_dict['deleted_at'], deleted_at_dict['job_link_path'])
                    
                    try:
                        mycursor.execute(sql,val)
                    except:
                        pass

                    for i in range(0, len(deleted_at_df)):
                        for z in groups.values():
                    
                            #Takes each Discord object and inserts into the following parameters.
                            name = z['name']
                            color = z['color']
                            webhook = z['webhook']
                            webhook_name = DiscordWebhook(url=webhook, username=name)

                            embed = DiscordEmbed(title="Job Removed from {}!".format(deleted_at_df['company'][i]),description="Title of the role is **{}.**".format(deleted_at_df['job_title'][i]),color=color)

                            embed.add_embed_field(name="Job Title",value=deleted_at_df['job_title'][i],inline=False)
                            embed.add_embed_field(name="Company",value=deleted_at_df['company'][i],inline=True)
                            embed.add_embed_field(name="Job Location",value=deleted_at_df['job_location'][i],inline=True)
                            embed.add_embed_field(name="Job Link",value='[Job Posting Link]({})'.format(deleted_at_df['job_link_path'][i]),inline=True)
                            embed.set_timestamp()
                            webhook_name.add_embed(embed)
                            
                            #Webhook is sent of the deleted job posting.
                            response = webhook_name.execute(remove_embeds=True)
                            
                            #Sleeping to prevent Discord rate limit of 50 messages every 15 second interval.
                            time.sleep(1)

#Finds the company token for front end api conversion.
def convert_job_link(url):
    company = url.split('/')[-1]
    api_link = 'https://boards-api.greenhouse.io/v1/boards/{}/jobs'.format(company)
    return api_link

#Timer Begins
start = timeit.default_timer()

#Temporarily store main urls and api urls to compare from database query.
company_query_list = []

for links in url_link:
    query_company = links.split('/')[-1]
    company_query_list.append(query_company)

for api in api_link:
    convert_job_link(links)
    query_company = api.split('/')[-3]
    company_query_list.append(query_company)

#mySQL query from the jobs table to compare current jobs.
mycursor.execute("SELECT distinct job_link_path, job_title, job_location, company, deleted_at FROM jobs WHERE company IN ({})".format(str(company_query_list)[1:-1]))

myresult = mycursor.fetchall()

#Takes query and converts into a list to compare.
url_list = []
query_dict = {}

for result in myresult:
    job_link_path, job_title, job_location, company, deleted_at = result

    query_dict[job_link_path] = {"job_title": job_title, "location": job_location, "company": company, "deleted_at": deleted_at}

    url_list.append(job_link_path)

url_list = set(url_list)

#Removed job listings function
check_removed_urls()


#Direct url function
for links in url_link:
    main(convert_job_link(links))

#API function
for b in api_link:
    main(b)

#commit all changes to the mySQL database.
# conn.commit()
mydb.commit()

# Prints the Start and End Time to Console
stop = timeit.default_timer()
print('\n' + 'Program Time Completed: ', stop - start)
