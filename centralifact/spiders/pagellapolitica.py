# -*- coding: utf-8 -*-
from scrapy.spiders import SitemapSpider
import extruct
import tldextract
import pandas as pd
from pprint import pprint
from scrapy.exceptions import CloseSpider
from urllib.parse import urlparse
import re
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import SQLAlchemyError
from centralifact import settings
import datetime
import polyglot
from polyglot.text import Text, Word

class claimReviewSpider(SitemapSpider):
	name='pagellapolitica'
	domainlang='it'
	def __init__(self,*args,**kwargs):
		self.sitemap_rules=eval(kwargs.get('sr'))
		self.sitemap_urls=eval(kwargs.get('su'))
		super(claimReviewSpider, self).__init__(*args, **kwargs) 

	def parse(self, response):
		claimdf=pd.DataFrame()
		#Extructing microdata or json in RDFA format 
		data=extruct.extract(response.text,response.url)
		#Domain Name
		domain=urlparse(response.url).netloc.strip('www').strip('.com')
		#Selecting Microdata
		selected=[properties for properties in data['microdata'] if properties['type']=='http://schema.org/ClaimReview']
		if selected:
			mode='micro'
		else:
			#If micro fails, selecting JSON
			try:
				selected=[properties for properties in data['json-ld'] if properties['@type']=='ClaimReview' or properties['@type']==["ClaimReview"]]
			except KeyError:
				selected=[properties for properties in data['json-ld'][0]['@graph'] if properties['@type']=='ClaimReview']
			mode='json'
		if selected:
			#If JSON or micro succeed
			for elements in selected:
				if mode=='micro':
					elements=elements['properties']
				for key in elements:
					if type(elements[key])==list:
						elements[key]=elements[key][0]
				##Flattening Dictionary
				scraped_data=pd.io.json.json_normalize(elements)
				##Renaming the columns of the dataframe
				scraped_data.columns=map(self.column_mapper,list(scraped_data.columns))
				##Dropping unimportant columns
				scraped_data=scraped_data.drop([None], axis=1)
				##Checking if fact_checker_name exists or review_author_name
				try:
					scraped_data.loc[:,'fact_checker_name']=scraped_data['fact_checker_name']
				except KeyError:
					try:
						scraped_data.loc[:,'fact_checker_name']=scraped_data['review_author_name']
					except KeyError:
						#As a last resort extracting domain name from the url
						domname=urlparse(scraped_data.loc[0,'fact_checker_url']).hostname.split('.')[1].capitalize()
						scraped_data['fact_checker_name']=domname
						scraped_data['review_author_name']=domname
				try:
					scraped_data.loc[:,'claim_text']=scraped_data['claim_text']
				except KeyError:
					scraped_data.loc[:,'claim_text']=scraped_data['claim_description']
					scraped_data=scraped_data.drop(['claim_description'], axis=1)
				##Appending to the dataframe
				claimdf=claimdf.append(scraped_data,ignore_index=True)
			##Filtering columns needed for Claim_review table
			claim_review=claimdf.filter(regex='(review_url|review_date|claimID|fact_checkerID|best_rating|worst_rating|rating_value|rating_name|review_author_name|review_rating_img|review_modified_date|review_headline|review_img|review_description)')
			claim_review.loc[:,'claimID']=0
			claim_review.loc[:,'fact_checkerID']=0
			claim_review.loc[:,'review_crawl_date']=str(datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
			claim=claimdf.filter(regex='(claim_text|claim_description|claim_author_name|claim_url|claim_date|claim_author_img|claim_author_job|claim_location|claim_location_url)')
			claim.loc[:,'claim_crawl_date']=str(datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
			##Filtering columns needed for Fact_checker table
			fact_checker=claimdf.filter(regex='(fact_checker_name|fact_checker_url|fact_checker_img)')
			####Creating MySQL Engine
			engine=create_engine(URL(**settings.DATABASE),connect_args={'charset':'utf8'})
			#################Checking if fact checker exists
			fact_checker_check=pd.read_sql_query('select * from fact_checker where fact_checker_name="%s"' %(fact_checker.loc[0,'fact_checker_name']),con=engine)
			if len(fact_checker_check)==0:
				###################If fact checker does not exist
				pd.DataFrame(fact_checker.iloc[0]).T.to_sql('fact_checker',engine,if_exists='append',index=False)
				fact_checker_check=pd.read_sql_query('select * from fact_checker where fact_checker_name="%s"' %(fact_checker.loc[0,'fact_checker_name']),con=engine)
				fact_checkerID=fact_checker_check.loc[0,'fact_checkerID']
			else:
				##Storing fact_checkerID for claim_review
				fact_checkerID=fact_checker_check.loc[0,'fact_checkerID']

			#############Iterating through the Claim_reiew Dataframe
			for i in range(len(claim_review)):
				################Checking if claim exists
				flag=0
				claim_review_check=pd.read_sql_query("select * from claim_review where review_url='%s'" %(claim_review.loc[i,'review_url']),con=engine)
				if len(claim_review_check)==0:
					#Row for this review does not exist
					flag=1
				else:
					#Row exists
					modified_date=claim_review_check.loc[0,'review_modified_date']
					try:
						if claim_review.loc[i,'review_modified_date']!=modified_date:
							#If review_modified_data has changed
							flag=1
					except KeyError:
						#If review_modified_data does not exist in the parsed dataframe
						if modified_date!=None:
							flag=1
				if flag==1:
					claim_check=pd.read_sql_query("select * from claim where claim_text='%s'" %(claim.loc[i,'claim_text'].replace("%","%%").replace("'","''")),con=engine)
					if len(claim_check)==0:
						#####################If claim does not exist
						pd.DataFrame(claim.iloc[i]).T.to_sql('claim',engine,if_exists='append',index=False)
						claim_check=pd.read_sql_query("select * from claim where claim_text='%s'" %(claim.loc[i,'claim_text'].replace("%","%%").replace("'","''")),con=engine)
						claimID=claim_check.loc[0,'claimID']
						claim_review.loc[i,'claimID']=claimID
						######Polyglot ner Language specific to spider and domain
						#extracting entities from claim text and setting lang
						text1=Text(claim.loc[i,'claim_text'].strip('"'))
						text1.language = domainlang
						try:
							#extracting entities from description and setting lang
							text2=Text(claim.loc[i,'claim_description'])
							text2.language = domainlang
							ner_entities=textpt1.entities+textpt2.entities
						except KeyError:
							#if description does not exist
							ner_entities=text1.entities
						for entity in ner_entities:
							entity_tag=str(entity.tag)
							entity=" ".join(entity).replace("\\","")
							entity_check=pd.read_sql_query('select * from entity where entity_text="%s"' %(entity),con=engine)
							if len(entity_check)==0:
								pd.DataFrame([[entity_tag,entity]],columns=['type', 'entity_text']).to_sql('entity',engine,if_exists='append',index=False)
								entity_check=pd.read_sql_query('select * from entity where entity_text="%s"' %(entity),con=engine)
								entityID=entity_check.loc[0,'entityID']
								pd.DataFrame([[entityID,claimID]],columns=['entityID','claimID']).to_sql('claim_entity',engine,if_exists='append',index=False)
							else:
								entityID=entity_check.loc[0,'entityID']
								pd.DataFrame([[entityID,claimID]],columns=['entityID','claimID']).to_sql('claim_entity',engine,if_exists='append',index=False)
						##############################################
					else:
						claimID=claim_check.loc[0,'claimID']
						claim_review.loc[i,'claimID']=claimID
					claim_review.loc[i,'fact_checkerID']=fact_checkerID
					pd.DataFrame(claim_review.iloc[i]).T.to_sql('claim_review',engine,if_exists='append',index=False)
		return claimdf.to_dict()
	def column_mapper(self,text):
		try:
			d={
			bool(re.match('url',text)):'review_url',
			bool(re.match('datePublished',text)):'review_date',
			bool(re.match('dateModified',text)):'review_modified_date',
			bool(re.match('image.(\\w+\\.)?url',text)):'review_img',
			bool(re.match('headline',text)):'review_headline',
			bool(re.match('author.(\\w+\\.)?name',text)):'review_author_name',
			bool(re.match('reviewRating.(\\w+\\.)?ratingValue',text)):'rating_value',
			bool(re.match('reviewRating.(\\w+\\.)?bestRating',text)):'best_rating',
			bool(re.match('reviewRating.(\\w+\\.)?worstRating',text)):'worst_rating',
			bool(re.match('reviewRating.(\\w+\\.)?alternateName',text)):'rating_name',
			bool(re.match('reviewRating.(\\w+\\.)?image',text)):'review_rating_img',
			bool(re.match('author.name',text)):'fact_checker_name',
			bool(re.match('author.(\\w+\\.)?image',text)):'fact_checker_img',
			bool(re.match('author.(\\w+\\.)?url',text)):'fact_checker_url',
			bool(re.match('description',text)):'claim_description',
			bool(re.match('claimReviewed',text)):'claim_text',
			bool(re.match('itemReviewed.(\\w+\\.)?datePublished',text)):'claim_date',
			bool(re.match('itemReviewed.(\\w+\\.)?author.(\\w+\\.)?jobTitle',text)):'claim_author_job',
			bool(re.match('itemReviewed.(\\w+\\.)?author.(\\w+\\.)?name',text)):'claim_author_name',
			bool(re.match('itemReviewed.(\\w+\\.)?image',text)):'claim_author_img',
			bool(re.match('itemReviewed.(\\w+\\.)?name',text)):'claim_location',
			bool(re.match('(\\w+\\.)*sameAs',text)):'claim_location_url'
			}
			return d[True]
		except KeyError:
			return