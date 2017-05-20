#################################################################

    l_sell_stock = []
    l_sell_amount = []
    file_name = 'C:/Users/Administrator/Desktop/wyk/sell.xlsx'
    df = pd.read_excel(file_name)
    for index, row in df.iterrows():
        stock_id = str(row[0])
        stock_id = stock_id.zfill(6)
        amount = row["amount"]
        l_sell_stock.append(stock_id)
        l_sell_amount.append(amount)

##############################################################
##############################################################
##############################################################
			
###############################################################
