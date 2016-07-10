# Campus Ethnic Diversity
# http://colleges.usnews.rankingsandreviews.com/best-colleges/rankings/national-universities/campus-ethnic-diversity?src=stats

import urllib.request
import re
import string
import io

# list of urls from which to scrape data 
website = ["http://colleges.usnews.rankingsandreviews.com/best-colleges/rankings/national-universities/campus-ethnic-diversity?src=stats", "http://colleges.usnews.rankingsandreviews.com/best-colleges/rankings/national-universities/campus-ethnic-diversity/page+2", "http://colleges.usnews.rankingsandreviews.com/best-colleges/rankings/national-universities/campus-ethnic-diversity/page+3", "http://colleges.usnews.rankingsandreviews.com/best-colleges/rankings/national-universities/campus-ethnic-diversity/page+4", "http://colleges.usnews.rankingsandreviews.com/best-colleges/rankings/national-universities/campus-ethnic-diversity/page+5", "http://colleges.usnews.rankingsandreviews.com/best-colleges/rankings/national-universities/campus-ethnic-diversity/page+6", "http://colleges.usnews.rankingsandreviews.com/best-colleges/rankings/national-universities/campus-ethnic-diversity/page+7", "http://colleges.usnews.rankingsandreviews.com/best-colleges/rankings/national-universities/campus-ethnic-diversity/page+8", "http://colleges.usnews.rankingsandreviews.com/best-colleges/rankings/national-universities/campus-ethnic-diversity/page+9", "http://colleges.usnews.rankingsandreviews.com/best-colleges/rankings/national-universities/campus-ethnic-diversity/page+10", "http://colleges.usnews.rankingsandreviews.com/best-colleges/rankings/national-universities/campus-ethnic-diversity/page+11" ]

# create lists of university name, tuition rates, and total enrollment 
university_name = []
ethnic_diversity = []

count = 0
# regular expressions to scrub each column of data: university name, tuition rates, and total enrollment
rex_university_name ='<a class="school-name" href="/best-colleges/.+?">(.+?)</a>'
rex_ethnic_diversity = 'title=".1.0=highest.">\s*(.*?)\s*<span'

# compile collected data
compiled_ethnic_diversity = re.compile(rex_ethnic_diversity)
compiled_university_name = re.compile(rex_university_name)

# remove html format
while count < len(website):
    html_file = urllib.request.urlopen(website[count])
    html_text = html_file.read().decode('iso-8859-1')
    # remove non-english strings from university name
    remove_html_string = html_text.replace('&mdash;â\x80\x8b',' ')
    # add data to lists
    university_name = university_name + re.findall(compiled_university_name , remove_html_string)
    ethnic_diversity = ethnic_diversity + re.findall(compiled_ethnic_diversity , html_text) 
    count += 1

# create a data dictionary for storing data lists 
data_dict = {university_name[j]:ethnic_diversity[j] for j in range(len(university_name))}

# write output to data file 
data_file = io.open('/Users/Clare/Documents/University of Texas/Senior UT/CS327/Project Scrape/ethnic_diversity.txt', 'w',encoding='utf8')
data_file.write(str(data_dict))

# print when process complete 
print ("Data successfully collected in folder.")