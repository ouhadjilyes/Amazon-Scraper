import pandas as pd
from bs4 import BeautifulSoup
import requests
import csv
import time
from multiprocessing import Manager
import multiprocessing
import lxml.html
import concurrent.futures
from csv import writer


def search_isbns(i):

    # web scraping API url
    ENDPOINT = "https://api.webscrapingapi.com/v1"

    url = "https://www.amazon.de/s?k=" + i + "&language=de_DE" # Product Url

    # API options  
    params = {
        "api_key": api_key,
        "url": url, 
        "proxy_type":"datacenter"
    }

    try:
        # request to check if a product exist with the specified ISBN
        response = requests.request("GET", ENDPOINT, params=params)
        soup = BeautifulSoup(response.text, "lxml")
    except Exception as E:
        print(E)

    if "Keine Ergebnisse für" in soup.text: # the product doesn't exist on amazon
        print("Scraping " + str(i) + " isbn... Not Found !")
        result = "0 results"
        price = ""
        condition = ""
        delivery_time = ""
        delivery_cost = ""
        results = [url," " + i, result, price, condition, delivery_time, delivery_cost]
    else:
        print("Scraping " + str(i) + " isbn... Available !")
        # get product unique link
        d = soup.find("div", {"class": "a-section a-spacing-small a-spacing-top-small"}).text.strip()
        result = d.split(" für")[0]
        divi = soup.find("div", {"class": "s-result-item s-asin sg-col-0-of-12 sg-col-16-of-20 sg-col s-widget-spacing-small sg-col-12-of-16"})
        if divi is None:
            link = ""
            i = ""
            price = ""
            condition = ""
            delivery_time = ""
            delivery_cost = ""
            sold_by = ""
            asin = ""
            isbn13 = ""
            isbn10 = ""
        else:
            link = divi.find("h2", {"class": "a-size-mini a-spacing-none a-color-base s-line-clamp-2"}).find("a")["href"]
            i = str(link).split("?keywords=")[-1].split("&")[0]

            # product full url
            url = "https://www.amazon.de" + str(link) + "&language=de_DE"

            # API Options 
            params = {
                "api_key": api_key,
                "url": url,
                "proxy_type":"datacenter"  
            }

            try:
                # Second webcrapingapi request to get product details
                response = requests.request("GET", ENDPOINT, params=params)
                soup = BeautifulSoup(response.text, "lxml")

            except Exception as E:
                print(E)

            # parsing response to get product info 
            try:
                ul = soup.find("div", {"id": "detailBullets_feature_div"}).find("ul", {
                    "class": "a-unordered-list a-nostyle a-vertical a-spacing-none detail-bullet-list"})
                lis = ul.find_all("li")
                for li in lis:
                    if "ASIN" in li.text:
                        asin = str(li.text.strip()).split(" ")[-1]
                    if "ASIN" not in str(lis):
                        asin = ""
                    if "ISBN-13" in li.text:
                        isbn13 = str(li.text.strip()).split(" ")[-1]
                    if "ISBN-13" not in str(lis):
                        isbn13 = ""
                    if "ISBN-10" in li.text:
                        isbn10 = str(li.text.strip()).split(" ")[-1]
                    if "ISBN-10" not in str(lis):
                        isbn10 = ""
            except:
                asin = ""
                isbn13 = ""
                isbn10 = ""
            try:
                soldby = soup.find("div", {"id": "merchant-info"})
                if "Versand durch Amazon" in str(soldby):
                    sold_by = "AMZ"
                    try:
                        try:
                            try:
                                price = soup.find("span", {"class": "a-price aok-align-center"}).find("span",
                                                                                                    {
                                                                                                        "class": "a-offscreen"}).text
                                price = str(price).replace(",", ".").replace("€", "")
                            except:
                                price = soup.find("span",
                                                {
                                                    "class": "a-price a-text-price header-price a-size-base a-text-normal"}).find(
                                    "span",
                                    {
                                        "class": "a-offscreen"}).text
                                price = str(price).replace(",", ".").replace("€", "")
                        except:
                            price = soup.find("span", {"id": "price"}).text
                            price = str(price).split("\xa0")[0].replace(",", ".")
                    except:
                        price = ""
                    try:
                        try:
                            condition = soup.find("div", {"id": "usedBuySection"}).find("span", {"class": "a-text-bold"}).text
                            condition = str(condition).split(":")[0]
                            if str(condition) == "Tweedehands":
                                box = soup.find("div", {"id": "usedbuyBox"})
                                condition = box.find("div", {"class": "a-section a-spacing-base"}).text.strip()
                                condition = str(condition).split("Tweedehands:")[-1].replace(" ", "").split("|")[0]
                            elif str(condition) == "Gebraucht":
                                box = soup.find("div", {"id": "usedbuyBox"})
                                condition = box.find("div", {"class": "a-section a-spacing-base"}).text.strip()
                                condition = str(condition).split("Gebraucht:")[-1].replace(" ", "").split("|")[0]
                        except:
                            try:
                                try:
                                    condition = soup.find("div", {'class': "a-column a-span4 a-text-left"}).text.strip()
                                    condition = str(condition).split(":")[0]
                                except:
                                    condition = \
                                        soup.find("div", {'id': "olp_feature_div"}).find("span",
                                                                                        {
                                                                                            "data-action": "show-all-offers-display"}).a[
                                            "href"]
                                    condition = str(condition).split("condition=")[-1].lower()
                            except:
                                condition = soup.find("div", {"id": "newAccordionCaption_feature_div"}).text.strip()
                        if str(condition) == "all":
                            condition = "neu"
                    except:
                        condition = ""
                    try:
                        delivery = soup.find("div", {"id": "mir-layout-DELIVERY_BLOCK"})
                    except:
                        print(url)
                    try:
                        delivery_cost = str(delivery.text.strip()).split(" ")[0]
                    except:
                        delivery_cost = ""
                    if delivery_cost != "KOSTENLOSE":
                        try:
                            delivery_cost = str(delivery.text.strip()).split("€")[0].split(" ")[-1]
                        except:
                            delivery_cost = ""
                    try:
                        delivery_time = delivery.find("span", {"class": "a-text-bold"}).text
                    except:
                        delivery_time = ""
                else:
                    sold_by = "Partner"
                    try:
                        try:
                            try:
                                try:
                                    price = soup.find("span", {"class": "a-price aok-align-center"}).find("span",
                                                                                                        {
                                                                                                            "class": "a-offscreen"}).text
                                    price = str(price).replace(",", ".").replace("€", "")
                                except:
                                    price = soup.find("span",
                                                    {
                                                        "class": "a-price a-text-price header-price a-size-base a-text-normal"}).find(
                                        "span",
                                        {
                                            "class": "a-offscreen"}).text
                                    price = str(price).replace(",", ".").replace("€", "")
                            except:
                                price = soup.find("span", {"id": "price"}).text
                                price = str(price).split("\xa0")[0].replace(",", ".")
                        except:
                            price = soup.find("span",
                                            {"class": "a-size-base a-color-price offer-price a-text-normal"}).text.strip()
                            price = str(price).split("\xa0")[0].replace(",", ".")
                    except:
                        price = ""
                    try:
                        try:
                            condition = soup.find("div", {"id": "usedBuySection"}).find("span", {"class": "a-text-bold"}).text
                            condition = str(condition).split(":")[0]
                            if str(condition) == "Tweedehands":
                                box = soup.find("div", {"id": "usedbuyBox"})
                                condition = box.find("div", {"class": "a-section a-spacing-base"}).text.strip()
                                condition = str(condition).split("Tweedehands:")[-1].replace(" ", "").split("|")[0]
                            elif str(condition) == "Gebraucht":
                                box = soup.find("div", {"id": "usedbuyBox"})
                                condition = box.find("div", {"class": "a-section a-spacing-base"}).text.strip()
                                condition = str(condition).split("Gebraucht:")[-1].replace(" ", "").split("|")[0]
                        except:
                            try:
                                try:
                                    condition = soup.find("div", {'class': "a-column a-span4 a-text-left"}).text.strip()
                                    condition = str(condition).split(":")[0]
                                except:
                                    condition = \
                                        soup.find("div", {'id': "olp_feature_div"}).find("span", {
                                            "data-action": "show-all-offers-display"}).a[
                                            "href"]
                                    condition = str(condition).split("condition=")[-1].lower()
                            except:
                                condition = soup.find("div", {"id": "newAccordionCaption_feature_div"}).text.strip()
                        if str(condition) == "all":
                            condition = "neu"
                    except:
                        condition = ""
                    try:
                        delivery = soup.find("div", {"id": "mir-layout-DELIVERY_BLOCK"})
                    except:
                        print(url)
                    try:
                        delivery_cost = str(delivery.text.strip()).split(" ")[0]
                    except:
                        delivery_cost = ""
                    if delivery_cost != "KOSTENLOSE":
                        try:
                            delivery_cost = str(delivery.text.strip()).split("€")[0].split(" ")[-1]
                        except:
                            delivery_cost = ""
                    try:
                        delivery_time = delivery.find("span", {"class": "a-text-bold"}).text
                    except:
                        delivery_time = ""
            except:
                price = ""
                condition = ""
                delivery_time = ""
                delivery_cost = ""
                sold_by = ""
        
        results = [url, " " + i, result, price, condition, delivery_time, delivery_cost, sold_by, asin, isbn13, isbn10]

    with open('Output-AMZ.csv', 'a', newline="") as f_object:
        writer_object = writer(f_object)
        writer_object.writerow(results)
    #return i

def main(Threads, isbns):
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=Threads) as executor:
        executor.map(search_isbns, isbns)
        #print([val for val in executor.map(search_isbns, isbns)])

def SetUp(input_file , output_file):

    Threads = int(input("Number of Threads : "))

    print("Reading ISBNs from input sample...")
    # read all ISBNs to a list
    dataframe = pd.read_excel(input_file, dtype=str)
    values = dataframe['ISBNS:'].tolist()
    isbns = []
    for isbn in values:
        if str(isbn) != "nan":
            isbns.append(isbn)
    
    print("Creating output file...")
    # cleaner code to create the output file
    with open(output_file, 'w' , newline="" , encoding='utf-8-sig') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerow(
            ["Amazon-url:", "ISBN/EAN:", "Results", "Price", "Condition:", "(Estimated) delivery-time:", "Delivery-cost:",
                "AMZ or Partner:", "ASIN:", "ISBN-13:", "ISBN-10:"]
        )
    print("Done !!")
    main(Threads, isbns)

if __name__ == '__main__':
    print("Singup for a free trial in webscrapingapi.com and get the API KEY!")
    api_key = str(input("Enter your api-key : "))
    SetUp('Sampleinputamazonscraper.xlsx', 'Output-AMZ.csv')
    print("Done!")