from io import StringIO
import pandas as pd


def getTemporalTable(csv, q_table='ALL', occurence='50%'):
    
    # verify q_table is one of the following: ['FIRST-QUARTILE', 'SECOND-QUARTILE', 'THIRD-QUARTILE', 'FOURTH-QUARTILE', 'ALL']
    if q_table not in ['FIRST-QUARTILE', 'SECOND-QUARTILE', 'THIRD-QUARTILE', 'FOURTH-QUARTILE', 'ALL']:
        raise ValueError(f"q_table must be one of the following: 'FIRST-QUARTILE', 'SECOND-QUARTILE', 'THIRD-QUARTILE', 'FOURTH-QUARTILE', 'ALL'")
    
    # verify occurence is one of the following: ['90%','80%','70%','60%','50%','40%','30%','20%','10%']
    if occurence not in ['90%','80%','70%','60%','50%','40%','30%','20%','10%']:
        raise ValueError(f"occurence must be one of the following: ['90%','80%','70%','60%','50%','40%','30%','20%','10%']") 

    # pd.read_csv("data\se_1_24h_temporal.csv")
    with open(csv, "r") as f:
        data = f.readlines()
    
    table_start_indexes = [i for i,v in enumerate(data) if f"CUMULATIVE PERCENTAGES OF TOTAL PRECIPITATION FOR" in v]
    table_titles = [v for i,v in enumerate(data) if "CUMULATIVE PERCENTAGES OF TOTAL PRECIPITATION FOR" in v]
    table_titles = [v.split("CUMULATIVE PERCENTAGES OF TOTAL PRECIPITATION FOR")[-1].rstrip("CASES\n").strip() for v in table_titles]

    length_tables = len(table_start_indexes)
    for i,table in enumerate(table_start_indexes):
        # table_headers are +2 from the table_start_index
        table_header_index = table + 2
        table_title = table_titles[i]
        # ensure not at end of table before using the next table start index.
        if i < length_tables - 1:
            table = data[table_header_index:table_start_indexes[i+1]]
            table = [v.rstrip("\n") for v in table]
            # print (*table)
            df_table = pd.read_csv(StringIO("\n".join(table)), sep=",", header=0)
        else: # last table just grabs to end of file
            table = data[table_header_index:]
            table = [v.rstrip("\n") for v in table]
            df_table = pd.read_csv(StringIO("\n".join(table)), sep=",", header=0)
        
        if table_title == q_table:
            # print (df_table)
            return df_table[['hours',occurence]]

if __name__ == '__main__':
    d_table = getTemporalTable(r"data\Texas\tx_3_24h_temporal.csv", q_table='ALL', occurence='50%')
    print (d_table)