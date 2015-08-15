"""
    Morning star finance stats scraping.
    Retrieve the information based on the export to csv button in morning star finance web page.

    Updates:
        Jul 25 2015: Add in MS_ValuationExtract class
        Feb 09 2015: Resolve joining problem.

    Learnings:  
        How to make selenium execute the java script.
        http://stackoverflow.com/questions/2767690/how-do-you-use-selenium-to-execute-javascript-within-a-frame
        http://stackoverflow.com/questions/25209523/selenium-python-javascript-execution

        downloading a file
        http://stackoverflow.com/questions/22676/how-do-i-download-a-file-over-http-using-python
        http://financials.morningstar.com/ajax/exportKR2CSV.html?&callback=?&t=XSES:N4E&region=sgp&culture=en-US&cur=&order=asc

        XHR requests
        http://financials.morningstar.com/financials/getKeyStatPart.html?&callback=jsonp1423036133322&t=XSES:N4E&region=sgp&culture=en-US&cur=&order=asc&_=1423036135111
        using chrome developer tools to see the respective XHR
        http://financials.morningstar.com/financials/getFinancePart.html?&callback=jsonp1423036133321&t=XSES:N4E&region=sgp&culture=en-US&cur=&order=asc&_=1423036135109

        Update two dicts
        http://stackoverflow.com/questions/38987/how-can-i-merge-two-python-dictionaries-in-a-single-expression

        pandas change index
        http://stackoverflow.com/questions/10457584/redefining-the-index-in-a-pandas-dataframe-object

        pandas to csv ascii error
        http://stackoverflow.com/questions/16923281/pandas-writing-dataframe-to-csv-file

        replace string in entire dataframe
        http://stackoverflow.com/questions/17142304/replace-string-value-in-entire-dataframe
        

    Todo:
        Add in min P/E?? average P/E across time or 3 year ago P/E (not able to show the historic P/E
        cannot scrape historic pE or coupled with the price to get the ratio??

    Bugs:
        problem of unable to append maybe due to duplication of columns
        https://github.com/pydata/pandas/issues/3487


"""


import re, os, sys, math, time, datetime, shutil
import pandas
from pattern.web import URL, DOM, plaintext, extension, Element, find_urls

class MS_StatsExtract(object):
    """ 
        Using morning star ajax call.
        Can only get one stock at a time.
    """
    def __init__(self):
        """ List of url parameters -- for url formation """
        self.com_data_start_url = 'http://financials.morningstar.com/ajax/exportKR2CSV.html?&callback=?&t=XSES:'
        self.com_data_stock_portion_url = '' 
        self.com_data_stock_portion_additional_url = ''# for adding additonal str to the stock url.
        self.com_data_end_url = '&region=sgp&culture=en-US&cur=&order=asc'
        self.com_data_full_url = ''
        self.stock_list = ''#list of stock to parse. 

        ## printing options
        self.__print_url = 0

        ## temp csv storage path
        self.ms_stats_extract_temp_csv = r'c:\data\temp\ms_stats.csv'
        self.ms_stats_extract_temp_csv_transpose = r'c:\data\temp\ms_stats_t.csv'

        ## Temp Results storage
        self.target_stock_data_df = object() 
        
        ## full result storage
        self.com_data_allstock_df = pandas.DataFrame()
        self.hist_company_data_trends_df = pandas.DataFrame()

    def set_stock_sym_append_str(self, append_str):
        """ Set additional append str to stock symbol when forming stock url.
            Set to sel.cur_quotes_stock_portion_additional_url.
            Mainly to set the '.SI' for singapore stocks.
            Args:
                append_str (str): additional str to append to stock symbol.
        """
        self.com_data_stock_portion_additional_url = append_str

    def set_target_stock_url(self, stock_sym):
        """ Set the target stock. Single stock again.
            Set to self.com_data_stock_portion_url
            Args:
                stock_sym (str): Stock symbol.
        """
        self.com_data_stock_portion_url = stock_sym

    def set_stocklist(self, stocklist):
        """ Set list of stocks to be retrieved.
            Args:
                stocklist (list): list of stocks to be retrieved.
        """
        self.stock_list = stocklist

    def form_url_str(self):
        """ Form the url str necessary to get the .csv file
            May need to segregate into the various types.
            Args:
                type (str): Retrieval type.
        """           
        self.com_data_full_url = self.com_data_start_url + self.com_data_stock_portion_url +\
                                   self.com_data_end_url

    def get_com_data(self):
        """ Combine the cur quotes function.
            Formed the url, download the csv, put in the header. Have a dataframe object.
            Each one is one stock.
        """
        self.form_url_str()
        if self.__print_url: print self.com_data_full_url
        
        ## here will process the data set
        self.downloading_csv()

    def downloading_csv(self):
        """ Download the csv information for particular stock.

        """
        self.download_fault = 0

        url = URL(self.com_data_full_url)
        f = open(self.ms_stats_extract_temp_csv, 'wb') # save as test.gif
        try:
            f.write(url.download())#if have problem skip
        except:
            if self.__print_download_fault: print 'Problem with processing this data: ', self.com_data_full_url
            self.download_fault =1
        f.close()

    def process_dataset(self):
        """ Processed the data set by converting the csv to dataframe and attached the information for various stocks.
            Before concat the df, change the header label first. especially for the differerent currency -->

        """

        ## Rows with additional headers are skipped
        try:
            #will try take care of commas in thousands
            self.target_stock_data_df =  pandas.read_csv(self.ms_stats_extract_temp_csv, header =2,thousands =',',
                                                         index_col = 0, skiprows = [19,20,31,41,42,43,48,58,53,64,65,72,73,95,101,102])
        except:
            print 'problem downloading files. '
        self.target_stock_data_df = self.target_stock_data_df.transpose().reset_index()
        self.target_stock_data_df["SYMBOL"] = self.com_data_stock_portion_url        
        #after transpose save back to same file and call again for column duplication problem
        self.target_stock_data_df.to_csv(self.ms_stats_extract_temp_csv_transpose, index =False)
        self.target_stock_data_df =  pandas.read_csv(self.ms_stats_extract_temp_csv_transpose)
        #rename columns
        self.target_stock_data_df.rename(columns={'Year over Year':'Revenue yoy','3-Year Average':'Revenue 3yr avg',
                                                '5-Year Average':'Revenue 5yr avg','10-Year Average':'Revenue 10yr avg',
                                                  
                                                'Year over Year.1':'Operating income yoy','3-Year Average.1':'Operating income 3yr avg',
                                                '5-Year Average.1':'Operating income 5yr avg','10-Year Average.1':'Operating income 10yr avg',

                                                'Year over Year.2':'Net income yoy','3-Year Average.2':'Net income 3yr avg',
                                                '5-Year Average.2':'Net income 5yr avg','10-Year Average.2':'Net income 10yr avg',
                                                  
                                                'Year over Year.3':'EPS yoy','3-Year Average.3':'EPS 3yr avg',
                                                '5-Year Average.3':'EPS 5yr avg','10-Year Average.3':'EPS 10yr avg',},
                                       inplace =True) 
        

        #the concat need to handle different currenctly or convert auto to singapore currency??
        """ Rename input before concat"""
        self.rename_columns_for_key_parameters()
        self.add_parameters_for_target_stock_df()

        if len(self.com_data_allstock_df) == 0:
            self.com_data_allstock_df = self.target_stock_data_df
        else:
            self.com_data_allstock_df = pandas.concat([self.com_data_allstock_df,self.target_stock_data_df],ignore_index =True) 

    def rename_columns_for_key_parameters(self):
        """ rename columns in self.target_stock_data_df"""
        col_list  = self.target_stock_data_df.columns.tolist()
        target_cols_with_currency_dff = [ n for n in col_list if re.search('\w+ \w+ Mil',n)]
        currency_unit =  re.search('\w+ (\w+) Mil',target_cols_with_currency_dff[0]).group(1) #take any one to extract
        self.target_stock_data_df['currency_unit'] = currency_unit

        ## for the columns rename
        rename_cols = {n:n.replace(' '+ currency_unit,'') for n in target_cols_with_currency_dff}

        ## more columns
        add_target_cols_with_currency_dff = [ n for n in col_list if re.search('Earnings Per Share|Dividends',n)]
        rename_cols_2 = {n:n.replace(' '+ currency_unit,'') for n in add_target_cols_with_currency_dff}

        rename_cols.update(rename_cols_2)
    
        ## rename the columns in self.target_stock_data_df
        self.target_stock_data_df =  self.target_stock_data_df.rename(columns = rename_cols)

    def add_parameters_for_target_stock_df(self):
        """ Add addtional paramters to the target stock df. Primiarly because the some of the morning star data lack certain resolutions
            need to take care of commmas

        """
        self.target_stock_data_df['Earnings Per Share cal'] = self.target_stock_data_df['Net Income Mil']/self.target_stock_data_df['Shares Mil']
        self.target_stock_data_df['Dividends cal'] = self.target_stock_data_df['Earnings Per Share cal']*self.target_stock_data_df['Payout Ratio %']
        self.target_stock_data_df['Return on Equity'] = self.target_stock_data_df['Net Income Mil']/self.target_stock_data_df["Total Stockholders' Equity"]

        #add the year columns
        self.target_stock_data_df['Year'] =  self.target_stock_data_df['index'].apply(lambda x: x[:4])

    def get_com_data_fr_all_stocks(self):
        """ Cater for all stocks. Each stock is parse one at a time.
        """
        self.com_data_allstock_df = pandas.DataFrame()
        
        for stock in self.stock_list:
            try:
                print 'Processing stock:', stock
                self.set_target_stock_url(stock)
                self.get_com_data()
                self.downloading_csv()
                self.process_dataset()
            except:
                print 'Problem with stock: ', stock

    ## process the data, group by each symbol and take the last 3-5 years EPS year on year??
    def get_trend_data(self): 
        """ Use for getting trends data of the dataset.
            Separate to two separate type. One is looking at gain in yoy gain, which means the gain of EPS eg is higher this year over the last as
            compared to the EPS gain of last year over the previous one.
            The other is positive gain which look for gain of company over year.
            may have accel growth if starting is negative
            
        """
        grouped_symbol = self.com_data_allstock_df.groupby("SYMBOL")

        self.hist_company_data_trends_df = pandas.DataFrame()
        for label in ['EPS yoy','Revenue yoy','Net income yoy']:
            for n in range(9,5,-1):
                if n == 9:
                    prev_data = grouped_symbol.nth(n)[label]
                    accel_growth_check = (prev_data == prev_data) #for EPS growht increase every eyar
                    normal_growth_check =  (prev_data >0) #for normal increase
                    continue
                current_data = grouped_symbol.nth(n)[label]
                accel_growth_check = accel_growth_check & (current_data <= prev_data)
                normal_growth_check = normal_growth_check & (current_data >0) 
                prev_data = current_data
                
            accel_growth_check = accel_growth_check.to_frame().rename(columns = {label: label + ' 4yr_accel'}).reset_index()
            normal_growth_check = normal_growth_check.to_frame().rename(columns = {label: label + ' 4yr_grow'}).reset_index()

            both_check_df =  pandas.merge(accel_growth_check, normal_growth_check, on = 'SYMBOL' )

            if len(self.hist_company_data_trends_df) ==0:
                self.hist_company_data_trends_df = both_check_df
            else:
                self.hist_company_data_trends_df = pandas.merge(self.hist_company_data_trends_df, both_check_df, on = 'SYMBOL' )

    def modify_stock_sym_in_df(self):
        """ Modify the stock sym in df especially for the Singapore stock where it require .SI to join in some cases.

        """
        self.hist_company_data_trends_df['SYMBOL']= self.hist_company_data_trends_df['SYMBOL'].astype(str) +'.SI'

    def strip_additional_parm_fr_stocklist(self, stocklist, add_parm = '.SI'):
        """ Strip the addtional paramters from the stock list. True in case where the input is XXX.SI and morning star do not required the additioanl SI.
            Args:
                stocklist (list): list of stock sym.
            Kwargs:
                add_parm (str): string to omit (.SI)

        """
        return [re.search('(.*)%s'%add_parm, n).group(1) for n in stocklist]



class MS_ValuationExtract(object):
    """ 
        Using morning star valuation extract
        Can only get one stock at a time.
        Make it as dict so can allow mutliple pull
    """
    def __init__(self):
        """"""
        #tuple of start url, mid url, end url , mid url can be empty
        self.retrieval_url_dict = {
                                    'valuation':('http://financials.morningstar.com/valuation/valuation-history.action?&t=XSES:','',
                                                 '&region=sgp&culture=en-US&cur=&type=price-earnings&_=1427535341054'),

                                    }
        self.retrieval_type = 'valuation'

        #result storage
        self.combined_valuation_df = pandas.DataFrame()
        
        ## printing options
        self.__print_url = 0

    def set_stocklist(self, stock_list):
        """ Set the list of stocks
            stock_list (list): list of stock symbol
        """
        self.stock_list = stock_list

    def set_retrieval_type(self, ret_type):
        """ Set  the url retrieval type.
            Set to self.retrieval_type.
        """
        self.retrieval_type = ret_type

    def set_target_stock_url(self, stock_sym):
        """ Set the target stock. Single stock again.
            Set to self.com_data_stock_portion_url
            Args:
                stock_sym (str): Stock symbol.
        """
        self.stock_portion_url = stock_sym

    def form_url_str(self):
        """ Form the url str necessary to get the url formation dependng on the retrieval type.
            Determine by the self.retrieval_type.
            
        """
        start_url, mid_url, end_url = self.retrieval_url_dict[self.retrieval_type]
        self.target_full_url = start_url + self.stock_portion_url + mid_url + end_url

    def url_site_download(self):
        """ Download the csv information for particular stock depending on the retrieval type.
            Retrieval type determine by self.retrieval_type

            Return:
                (str): output html from url.

        """
        self.download_fault = 0
        self.form_url_str() 

        url = URL(self.target_full_url)
        try:
            return url.download()
        except:
            if self.__print_download_fault: print 'Problem with processing this data: ', self.target_full_url
            self.download_fault =1
            return None

    def process_valuation_for_single_stock(self, stock_sym):
        """ Process the valuation information for Single Stock.
            Args:
                stock_sym (str): particular stock sym

            Return:
                (Pandas Dataframe): dataframe of particular stock valuation df
        """
        self.set_retrieval_type('valuation')
        self.set_target_stock_url(stock_sym)
        url_content = self.url_site_download()
        if url_content:
            return self.parse_valuation_data(url_content)

        return None
        

    def parse_valuation_data(self, url_html):
        """ Parse the valuation page for a particular stock and create a pandas dataframe for the particular stocks.
            Return:
                (Pandas Dataframe): dataframe of particular stock valuation df
        """

        w = pandas.io.html.read_html(url_html,tupleize_cols = True,header=0 )
        
        #select target rows. --> company historical data
        target_hist_com = w[0].iloc[[0,3,6,9]]

        # create the dict for renaming the columns
        org_col_list = ['Unnamed: ' + str(n) for n in range(1,12)] + ['Price/Earnings']
        rename_col_list = ['wp_history_'+ str(year) for year in range(2005,2015)] + ['wp_history_TTM'] + ['Valuation_type']
        rename_col_dict = {}
        for org_col, rename_col in zip(org_col_list, rename_col_list ):
            rename_col_dict[org_col] = rename_col
            
        # rename the columns 
        target_hist_com = target_hist_com.rename(columns=rename_col_dict)

        # rename the rows.
        for index, val_type in zip([0,3,6,9],['PE_ratio','PB','Price_sales_ratio','Price_cashflow_ratio']):
            target_hist_com['Valuation_type'][index]=val_type

        target_hist_com = target_hist_com.set_index('Valuation_type')
        
        #rename columns --> so set the year
        target_hist_rename_cols_dict = {n:n.strip('wp_history_') for n in target_hist_com.columns.tolist() }
        target_hist_com = target_hist_com.rename(columns = target_hist_rename_cols_dict)

        #Transpose the data set
        target_hist_com_t = target_hist_com.transpose()
        target_hist_com_t = target_hist_com_t.reset_index()
        target_hist_com_t = target_hist_com_t.rename(columns = {'index': 'Year'})

        # add in the symbol
        target_hist_com_t['SYMBOL'] = self.stock_portion_url #this will contain the stock symbol

        return target_hist_com_t


    def process_all_stock_data(self):
        """ Process all stock data for valuation results.
        """
        for stock in self.stock_list:
            print 'processing stock: ', stock
            temp_stock_df = self.process_valuation_for_single_stock(stock)
            if len(self.combined_valuation_df) == 0:
                self.combined_valuation_df = temp_stock_df
            else:
                self.combined_valuation_df =  self.combined_valuation_df.append(temp_stock_df)

        ## replace columns that do not have data '-'
        self.combined_valuation_df = self.combined_valuation_df.replace(u'\u2014','') #dash line


            
if __name__ == '__main__':

    choice  = 1

    if choice == 1:
        """ Combine both historical stats and valuation data"""

        # Get stock symbol from file
        # Or can input a series of stock Symbol
        file = r'C:\data\compile_stockdata\full_20150719.csv'
        full_stock_data_df = pandas.read_csv(file)
        stock_list = list(full_stock_data_df['SYMBOL'])
        stock_list = [n.strip('.SI') for n in stock_list]

        print 'Processing historical financial stats data'
        pp = MS_StatsExtract()
        pp.set_stocklist(stock_list)
        pp.get_com_data_fr_all_stocks()

        print 'Processing historical valuation data'
        yy = MS_ValuationExtract()
        yy.set_stocklist(stock_list)
        yy.process_all_stock_data()

        print 'Joining data set'
        combined_df = pandas.merge(pp.com_data_allstock_df, yy.combined_valuation_df, how = 'left', on = ['SYMBOL','Year'])

        # Join with additonal data from the compile data
        # Additional data have to retrieve from other sources.
        # Pls see: Retrieving stock news and Ex-date from SGX using python from https://simplypython.wordpress.com
        required_columns = ['SYMBOL','CompanyName','industry', 'industryGroup' ]
        partial_stock_df = full_stock_data_df[['SYMBOL','CompanyName','industry', 'industryGroup'] ]
        partial_stock_df['SYMBOL'] = partial_stock_df['SYMBOL'].str.strip('.SI')

        combined_df_2 = pandas.merge(combined_df, partial_stock_df, how = 'left', on = ['SYMBOL'])

        combined_df_2.to_csv(r'C:\data\temp\morn_star_data.csv', index = False)

    if choice ==2:

        ## Get stock symbol from file
        file = r'C:\data\compile_stockdata\full_20150521.csv'
        full_stock_data_df = pandas.read_csv(file)
        stock_list = list(full_stock_data_df['SYMBOL'])
        stock_list = [n.strip('.SI') for n in stock_list]

        pp = MS_StatsExtract()
        pp.set_stocklist(['BN4','BS6','N4E','U96','500','P13','S63'])
        #pp.set_stocklist(stock_list)
        #pp.set_stocklist(['D38'])
        pp.get_com_data_fr_all_stocks()
        pp.com_data_allstock_df.to_csv(r'C:\data\temp\morn_star_data.csv', index = False)
        #pp.get_trend_data()
        #pp.modify_stock_sym_in_df()
        #print pp.hist_company_data_trends_df

    if choice ==3:
        """ check on the historical valuation """
        print 'Processing historical valuation data'
        yy = MS_ValuationExtract()
        yy.set_stocklist(['BN4','BS6'])
        yy.process_all_stock_data()
        yy.combined_valuation_df.to_csv(r'C:\data\temp\morn_star_data1.csv', index = False)



    if choice ==4:
        filename = r'C:\data\full_Feb08.csv'
        pp = MS_StatsExtract()
        stock_df = pandas.read_csv(filename)
        pp.set_stocklist(pp.strip_additional_parm_fr_stocklist(list(stock_df['SYMBOL'])))
        pp.get_com_data_fr_all_stocks()
        pp.get_trend_data()
        pp.modify_stock_sym_in_df()
        full_data_df = pandas.merge(stock_df,pp.hist_company_data_trends_df, on = 'SYMBOL' )
        full_data_df.to_csv(filename, index ='False')






















