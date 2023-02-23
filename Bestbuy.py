from requests_html import HTMLSession
from datetime import datetime
import asyncio
import time
from datetime import date
import timeit
import sqlite3
import pandas as pd
from discord_webhook import DiscordWebhook, DiscordEmbed

start = time.perf_counter()

webhook = DiscordWebhook("https://discord.com/api/webhooks/1019080559869309020/a0e_qmUlb52hgd2Qq__enowzTLoDtDD0TPpyxKjXD8v3omnQ5-QeXB2y_EyrR3ZaGco5", username="Bot Bot Python")

csv_list = []

#ask for user input
search_term = 'formula 1 racing game'.replace(' ', '+')
# print(search_term)
# print(search_term)

# url = 'https://www.bestbuy.com/site/searchpage.jsp?cp=2&st=bike'
url = 'https://www.bestbuy.com/site/searchpage.jsp?cp={}&st={}'.format(1, search_term)
# print(url)
# s = AsyncHTMLSession()


def getData(url):
    s = HTMLSession()
    r = s.get(url)
    # Testing Data Line
    # title = r.html.find('.sku-item-list', first=True)
    url_list = []
    #locates last number in the search term list and converts string to integer
    try:
        range_index = r.html.find('.trans-button.page-number')[-1].text
        converted_num = int(range_index)+1
    except:
        converted_num = 2 #actual web page its scraping is page one due to indexing issue from retailer.

    total_search_count = r.html.find('.item-count', first=True).text
    print('Searching for {} with {} pages.'.format(total_search_count,converted_num-1))
    # print(url)

    #Checks each maximum web page based off the keyword.
    for i in range(1, converted_num):
        url = 'https://www.bestbuy.com/site/searchpage.jsp?cp={}&st={}'.format(i, search_term)
        r = s.get(url)
        title = r.html.find('.sku-item-list', first=True)
        print('\n''Starting on Page {}\n'.format(i))
        count = 0
        count_list = []

        #Code Begins Iterations
        for item in title.absolute_links:
            domain_checker = '{}'.format(item)
            retailer = domain_checker.partition('.')[2].partition('.')[0]
            link_checker = domain_checker.partition('#')[0]
            # print(link_checker)
            r = s.get(link_checker)
            # print(domain_checker)
            # print(retailer)
            # img = r.html.links.find('.image-link', first=True)

            # img = r.html.find('.image-link', first=False)
            # print(img.tag)
            # ratings = r.html.find('.ugc-c-review-average.font-weight-medium.order-1', first=True).text
            # print(ratings)
            # print(item)
            url_list.append(link_checker)
            count = int(count) + 1
            count_list.append(count)
            # print(item)

            if retailer != 'bestbuy':
                pass
            else:
                #Data Preprocessing

                #Normalizes availablility options.
                try:
                    availability_stock = r.html.find('.fulfillment-add-to-cart-button', first=True).text
                    if availability_stock == None:
                        availability = ''
                        # print('THIS IS A NONE VALUE')
                    elif availability_stock == 'Add to Cart':
                        availability = 'In Stock'
                        # print('Available')
                    else:
                        availability = 'Out of Stock'
                        # print('Out of Stock')
                except:
                    #checks for combo add to cart buttons
                    availability_stock = r.html.find('.fulfillment-combo-add-to-cart-button', first=True).text
                    if availability_stock == None:
                        availability = ''
                        # print('THIS IS A NONE VALUE')
                    elif availability_stock == 'Add to Cart':
                        availability = 'In Stock'
                        # print('Available')
                    else:
                        availability = 'Out of Stock'
                        # print('Out of Stock')
                # Reviews Normalization
                if r.html.find('.visually-hidden', first=True).text == 'Be the first to write a review':
                    review = 'No Reviews'
                else:
                    review = r.html.find('.c-reviews.order-2', first=True).text.partition(' ')[0].translate(
                        ({ord('('): None}))

                # Ratings Normalization
                if r.html.find('.visually-hidden', first=True).text == 'Be the first to write a review':
                    ratings = 'No Ratings'
                elif r.html.find('.visually-hidden', first=True).text == 'Not yet reviewed':
                    ratings = 'No Ratings'
                else:
                    ratings = r.html.find('.ugc-c-review-average.font-weight-medium.order-1', first=True).text

                # On Sale Boolean
                if r.html.find('.pricing-price__regular-price', first=True) == None:
                    sale_status = 'false'
                    sale_price = ''
                else:
                    sale_status = 'true'
                    sale_price = r.html.find('.pricing-price__regular-price', first=True).text.partition('$')[2]

                #SKU Logic
                try:
                    sku_check = r.html.find('.sku.product-data', first=True).text.partition(':')[2]
                    if sku_check == None:
                        sku = ''
                        # print('THIS IS A NONE VALUE')
                    else:
                        sku = sku_check
                except:
                    # checks for combo add to cart buttons
                        sku = ''
                        pass

                # Cleans retailer url into string.
                domain = '{}'.format(item)
                retailer = domain.partition('.')[2].partition('.')[0]
                link_checker = domain_checker.partition('#')[0]

                #Creates add to cart button link for discord.
                cart_link = 'https://api.bestbuy.com/click/seanotify/{}/cart'.format(sku)

                #Creats image link
                image_sku = sku[0:4]
                image_link = 'https://pisces.bbystatic.com/image2/BestBuy_US/images/products/{}/{}_sd.jpg'.format(image_sku,sku)

                # Grabs Timestamp
                today = datetime.now()
                current_date = today.strftime("%Y-%m-%d %H:%M:%S")

                # Main Product for Database

                timestamp = current_date
                title = r.html.find('.heading-5.v-fw-regular', first=True).text
                price = r.html.find('.sr-only', first=True).text.partition('$')[2].replace(',', '')
                sku = sku
                department = r.html.find('.c-breadcrumbs-order-list', first=True).text.split('\n')[1]
                category = r.html.find('.c-breadcrumbs-order-list', first=True).text.split('\n')[-1]
                sale_price = sale_price
                on_sale = sale_status
                availability = availability
                reviews = review
                ratings = ratings
                retailer = retailer

                product = {
                    'timestamp': current_date,
                    'title': r.html.find('.heading-5.v-fw-regular', first=True).text,
                    'price': r.html.find('.sr-only', first=True).text.partition('$')[2].replace(',', ''),
                    'sku': sku,
                    'department': r.html.find('.c-breadcrumbs-order-list', first=True).text.split('\n')[1],
                    'category': r.html.find('.c-breadcrumbs-order-list', first=True).text.split('\n')[-1],
                    'sale_price': sale_price,
                    'on_sale': sale_status,
                    'availability': availability,
                    'reviews': review,
                    'ratings': ratings,
                    'retailer': retailer
                }
                # print(product)
                csv_list.append(product)

                #Discord Logic
                embed = DiscordEmbed(title=title, color='FF0000', url=link_checker)
                embed.set_thumbnail(url=image_link)
                embed.add_embed_field(name='SKU', value=sku, inline=True)
                embed.add_embed_field(name='Price', value=price, inline=True)
                # embed.add_embed_field(name='Sale Price', value=sale_price, inline=True)
                embed.add_embed_field(name='Availability', value=availability, inline=True)
                embed.set_footer(text='Sean Notify', icon_url='http://hollywoodlife.com/wp-content/uploads/2019/01/geoff-hamanishi-kassandra-admits-to-cheating-ftr.jpg')

                embed.set_timestamp()
                webhook.add_embed(embed)
                embed.add_embed_field(name='Other', value='[ATC]({})'.format(cart_link), inline=False)


            response = webhook.execute()
        print('\n''Completed Page {}\n'.format(i))

    print('Web Scrape Completed!')
    print('Scraper captured {} links.'.format(len(count_list)))
    # print(url_list)



getData(url)
#
# async def main(urls):
#     s = AsyncHTMLSession()
#     tasks = (getData(s, url))
#     return await asyncio.gather(tasks)

# df = s.run(getData(s, url))
# print(df)
# results = asyncio.run(main(url))

pd.set_option('display.max_columns',None)
df = pd.DataFrame(csv_list)
print(df.head())
# webhook.send(

end = time.perf_counter() - start
print(end)