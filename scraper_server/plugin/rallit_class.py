import os
import re
import time
import json
import random
import asyncio
import aiohttp
import boto3
import pandas as pd
from typing import List, Dict, Optional, Union, Any
from datetime import date
from bs4 import BeautifulSoup


class Scraper:
    """
    A web scraper class for extracting job listings from "rallit.com".

    Attributes:
        base_url: The base URL for the website to be scraped.
        selected_job: The job category to be scraped.
        jobs: A list to store the scraped job listings.
        user_agent_list: A list of user agents to randomize headers for web requests.
        headers: Headers for the web requests.
    """
    
    job_category : List[str] = [
        "BACKEND_DEVELOPER", 
        "FRONTEND_DEVELOPER",
        "SOFTWARE_ENGINEER",
        "ANDROID_DEVELOPER",
        "IOS_DEVELOPER",
        "CROSS_PLATFORM_DEVELOPER",
        "DATA_ENGINEER",
        "DATA_SCIENTIST",
        "DATA_ANALYST",
        "MACHINE_LEARNING",
        "DBA",
        "DEV_OPS",
        "INFRA_ENGINEER",
        "QA_ENGINEER",
        "SUPPORT_ENGINEER",
        "SECURITY_ENGINEER",
        "BLOCKCHAIN_ENGINEER",
        "HARDWARE_EMBEDDED_ENGINEER",
        "AGILE_SCRUM_MASTER"
    ]

    def __init__(self, base_url: str, selected_job: str) -> None:
        """Initializes the Scraper with the given base URL and job category."""
        self.base_url = base_url
        self.selected_job = selected_job
        self.jobs: List[Dict[str, Union[str, List[str]]]] = []
        self.user_agent_list: List[str] = [
            'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.83 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
        ]
        self.headers = {'User-Agent': random.choice(self.user_agent_list)}

    async def get_object_thread(self, start: int, end: int) -> List[Dict[str, Union[str, List[str]]]]:
        """Generates an aiohttp session and asynchronously scrapes job listings."""
        async with aiohttp.ClientSession() as session:
            tasks = [self.get_job_parser(p_index=p_index, session=session) for p_index in range(start, end+1)]
            await asyncio.gather(*tasks)

        return self.jobs

    async def get_job_parser(self, p_index: int, session: aiohttp.ClientSession) -> None:
        """Asynchronously parses the job listings on a given page and extracts relevant details."""
        url = f'{self.base_url}?job={self.selected_job}&jobGroup=DEVELOPER&pageNumber={p_index}'
        headers = self.headers

        async with session.get(url, headers=headers) as response:
            try:
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                ul_set = soup.find(class_='css-mao678')
                rows = ul_set.find_all('li')
                for row in rows:
                    a_tag = row.find('a')
                    if a_tag:
                        position_url = a_tag.attrs['href']
                        position_key_match = re.search(r'/positions/(\d+)', position_url)
                        if position_key_match:
                            position_key = position_key_match.group(1)
                            position_dict = await self.get_position_parser(position_key=position_key, session=session)
                            self.jobs.append(position_dict)

            except (aiohttp.ClientError, ValueError):
                await asyncio.sleep(2)

            except Exception:
                pass

    async def get_position_parser(self, position_key: str, session: aiohttp.ClientSession) -> Dict[str, Union[str, List[str]]]:
        """Asynchronously fetches details of a specific job position using its key."""
        position_url = f'https://www.rallit.com/api/v1/position/{position_key}'
        headers = self.headers

        async with session.get(position_url, headers=headers) as response:
            data = await response.json()
            data_detail = data.get('data', {})

            position_dict = {
                "job_id": data_detail.get('id'),
                "category": self.selected_job,
                "platform": "rallit",
                "company": data_detail.get('companyName'),
                "title": data_detail.get('title'),
                "preferred": data_detail.get('preferredQualifications'),
                "required": data_detail.get('basicQualifications'),
                "primary_responsibility": data_detail.get('responsibilities'),
                "url": f"https://www.rallit.com/positions/{data_detail.get('id')}",
                "end_at": data_detail.get('endedAt'),
                "skills": data_detail.get('jobSkillKeywords'),
                "location": data_detail.get('addressMain'),
                "body": data_detail.get('description'),
                "company_description": data_detail.get('companyDescription'),
                "welfare": data_detail.get('benefits'),
                "coordinate": [data_detail.get('latitude'), data_detail.get('longitude')] if data_detail.get('latitude') and data_detail.get('longitude') else None
            }

        return position_dict

    @staticmethod
    def paragraph_parsing(parsing_data: BeautifulSoup) -> Optional[str]:
        """Strips and extracts the first paragraph from the provided parsing data."""
        paragraphs = [par.text.strip() for par in parsing_data.find_all('p')]
        return paragraphs[0] if paragraphs else None

    def save_to_csv(self, filename: str = "rallit.csv") -> None:
        """Save the data to a CSV file."""
        df = pd.DataFrame(self.jobs)
        if 'company_name' in df.columns:
            columns = ['company_name'] + [col for col in df if col != 'company_name']
            df = df[columns]
        df.to_csv(filename, index=False, encoding="utf-8-sig")
    
    
    @staticmethod
    def save_to_parquet(data_list: List[Dict[str, Union[str, List[str]]]], filename: str = "rallit.parquet") -> str:
        """Saves the provided list of data to a Parquet file and returns the file path."""
        folder = 'static'
        file_path = os.path.join(folder, filename)
        
        # Create a DataFrame from the data_list
        df = pd.DataFrame(data_list)
        
        import pyarrow.parquet as pq
        import pyarrow as pa
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path)

        return file_path
    

    @staticmethod
    def upload_to_s3(file_path: str, bucket_name: str, access_key: str, secret_key: str, region_name: str) -> None:
        """Uploads the specified file to an AWS S3 bucket."""
        today = date.today()
        file_name = f"rallit/year={today.year}/month={today.month:02}/day={today.day:02}/rallit.parquet"
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)
        s3.upload_file(file_path, bucket_name, file_name)
        print(f"Successfully uploaded {file_path} to {bucket_name}/{file_name}")

