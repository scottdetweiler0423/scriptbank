from selenium import webdriver
import pandas as pd
import time
import csv
from datetime import timedelta

start_time = time.time()  # start clock

output_csv = 'Name.csv'

link_keywords = ['schedule', 'service', 'ServiceApptForm']


def deduplicate(duplicate):
    final_list = []
    for num in duplicate:
        if num not in final_list:
            final_list.append(num)
    return final_list


# Optional block for reading input csv
"""
file_reader =  open("UpdatedDealerURLs.csv", "r", encoding = 'ascii')
read = csv.reader(file_reader)
startlist = [x[1] for x in read if x]
start_url_list = []

for url in startlist:
    start_url_list.append(url)
"""

start_url_list = ['https://www.171nissan.com/']

for url in start_url_list:
    # url_start_time = time.time() #start clock
    driver = webdriver.Chrome()
    driver.get(url)
    elems = driver.find_elements_by_xpath("//a[@href]")  # find all href links in 'a' tag
    dealer_dict = {'dealer': [],
                   'provider': [],
                   's_url': [],
                   'scrapetime': [],
                   'tags': []
                   }

    possible_links = []  # initialize list for possible links

    for elem in elems:
        try:
            possible_links.append(elem.get_attribute("href"))
        except:
            pass
            driver.quit()
    driver.quit()

    suspect_url_list = []  # initialize list for links that contain link keyword

    for link in possible_links:
        if any(link_keyword in str(link) for link_keyword in link_keywords):
            try:
                suspect_url_list.append(link)
            except:
                pass

    suspect_url_list = deduplicate(suspect_url_list)  # call deduplicate function to remove duplicate links

    count_links = len(suspect_url_list)
    if count_links > 0:
        print('Scraping through ' + str(count_links) + ' links for starting link: ' + url)

        for s_url in suspect_url_list:
            print(s_url)

        # loop through urls with 'schedule' to search for provider urls contained in iframe tag under src attribute

        for s_url in suspect_url_list:
            s_url_start_time = time.time()  # start clock
            driver = webdriver.Chrome()
            driver.get(s_url)
            searched_s_url = []
            taglist = []
            searched_s_url.append(s_url)
            print(searched_s_url)

            i_links = []
            iframes = driver.find_elements_by_xpath("//iframe[@src]")
            for i in iframes:
                try:
                    i_links.append(i.get_attribute("src"))
                except:
                    pass

            s_links = []
            scripts_href = driver.find_elements_by_xpath("//script[@href]")
            for s in scripts_href:
                try:
                    s_links.append(s.get_attribute("href"))
                except:
                    pass

            s2_links = []
            scripts_src = driver.find_elements_by_xpath("//script[@src]")
            for s2 in scripts_src:
                try:
                    s2_links.append(s2.get_attribute("src"))
                except:
                    pass

            d_links = []
            divs = driver.find_elements_by_xpath("//div[@href]")
            for d in divs:
                try:
                    d_links.append(d.get_attribute("href"))
                except:
                    pass

            l_links = []
            linktags = driver.find_elements_by_xpath("//link[@href]")
            for l in linktags:
                try:
                    l_links.append(l.get_attribute("href"))
                except:
                    pass

            a_links = []
            atags = driver.find_elements_by_xpath("//a[@href]")
            for a in atags:
                try:
                    a_links.append(a.get_attribute("href"))
                except:
                    pass

            tags = i_links + s_links + s2_links + d_links + l_links + a_links
            # tags = deduplicate(tags)
            try:
                taglist.extend(tags)
            except:
                taglist.extend('No tags')
            driver.quit()

            s_url_elapsed_time_secs = time.time() - s_url_start_time  # end time for dealer url

            # writing dealer url, taglist, provider, and scrape time to dealer_dict
            taglist = deduplicate(taglist)

            for tag in taglist:
                dealer_dict['dealer'].append(url)
                dealer_dict['tags'].append(tag)

                if len(searched_s_url) > 0:
                    dealer_dict['s_url'].append(searched_s_url[0])
                else:
                    dealer_dict['s_url'].append('No suspect url')

                if 'flathat' in tag:
                    dealer_dict['provider'].append('DealerLogix')
                elif 'xtime' in tag:
                    dealer_dict['provider'].append('XTIME')
                elif 'dealer-fx' in tag:
                    dealer_dict['provider'].append('Dealer-Fx')
                elif 'cdkappts' in tag or 'ServiceEdge' in tag:
                    dealer_dict['provider'].append('CDK')
                elif 'autoloop' in tag:
                    dealer_dict['provider'].append('Autoloop')
                elif 'timehighway' in tag:
                    dealer_dict['provider'].append('Timehighway')
                elif 'reynolds' in tag:
                    dealer_dict['provider'].append('Reynolds')
                elif 'autopoint' in tag:
                    dealer_dict['provider'].append('Autopoint')
                elif 'service.eleadcrm' in tag:
                    dealer_dict['provider'].append('eLeads')
                elif 'dealersocket' in tag:
                    dealer_dict['provider'].append('Dealersocket')
                else:
                    dealer_dict['provider'].append('Not Found')

                dealer_dict['scrapetime'].append(str(timedelta(seconds=round(s_url_elapsed_time_secs))))

        # writing dealer_dict to csv
        df = pd.DataFrame(dealer_dict)
        print(df.head())
        with open(output_csv, 'a', newline='') as f:
            df.to_csv(f, header=False)
    else:
        s_url_start_time = time.time()  # start clock
        s_url_elapsed_time_secs = time.time() - s_url_start_time  # end time for dealer url

        dealer_dict['dealer'].append(url)
        dealer_dict['s_url'].append('No suspect url')
        dealer_dict['provider'].append('Not Found')
        dealer_dict['scrapetime'].append(str(timedelta(seconds=round(s_url_elapsed_time_secs))))
        dealer_dict['tags'].append('No suspect url')
        # dealer_dict['link_keyword'].append('Keywords yielded no results')

        # writing dealer_dict to csv
        df = pd.DataFrame(dealer_dict)
        print(df.head())
        with open(output_csv, 'a', newline='') as f:
            df.to_csv(f, header=False)

# total run time
elapsed_time_secs = time.time() - start_time

msg = "Total execution took: %s secs (Wall clock time)" % timedelta(seconds=round(elapsed_time_secs))

print(msg)

# Optional block for testing targeting of elemtn
"""
driver = webdriver.Chrome()
driver.get('https://scheduler2.dealer-fx.com/nissan/en-us/100352')

iframes = driver.find_element_by_tag_name("link").get_attribute("href")

print(iframes)
"""

# Optional block for breaking loop when keyword is found
"""
            #checking if key words are in tags list
            if any('flathat' in s for s in tags):
                break
            if any('xtime' in s for s in tags):
                break
            if any('dealer-fx' in s for s in tags):
                break
            if any ('cdkappts' in s for s in tags):
                break
            if any ('ServiceEdge' in s for s in tags):
                break
            if any ('autoloop' in s for s in tags):
                break
            if any ('timehighway' in s for s in tags):
                break
            if any ('Reynolds' in s for s in tags):
                break
            if any ('autopoint' in s for s in tags):
                break
            if any ('service.eleadcrm' in s for s in tags):
                break
            if any ('dealersocket' in s for s in tags):
                break            
"""
