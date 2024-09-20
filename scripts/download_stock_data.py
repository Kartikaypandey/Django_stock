import os
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
import logging


# Configure logging
logging.basicConfig(
    filename='/Users/kartikaypandey/Documents/django_projects/stockproject/scripts/cron_job.log',  # Log file path
    level=logging.INFO,  # Log level
    format='%(asctime)s - %(levelname)s - %(message)s'  # Log format
)

# def getTickers():
#     return [
#     "360ONE.NS", "3MINDIA.NS", "ABB.NS", "ACC.NS", "AIAENG.NS", "APLAPOLLO.NS", "AUBANK.NS", "AARTIIND.NS", "AAVAS.NS", "ABBOTINDIA.NS",
#     "ACE.NS", "ADANIENSOL.NS", "ADANIENT.NS", "ADANIGREEN.NS", "ADANIPORTS.NS", "ADANIPOWER.NS", "ATGL.NS", "AWL.NS", "ABCAPITAL.NS",
#     "ABFRL.NS", "AEGISLOG.NS", "AETHER.NS", "AFFLE.NS", "AJANTPHARM.NS", "APLLTD.NS", "ALKEM.NS", "ALKYLAMINE.NS", "ALLCARGO.NS",
#     "ALOKINDS.NS", "ARE&M.NS", "AMBER.NS", "AMBUJACEM.NS", "ANANDRATHI.NS", "ANGELONE.NS", "ANURAS.NS", "APARINDS.NS", "APOLLOHOSP.NS",
#     "APOLLOTYRE.NS", "APTUS.NS", "ACI.NS", "ASAHIINDIA.NS", "ASHOKLEY.NS", "ASIANPAINT.NS", "ASTERDM.NS", "ASTRAZEN.NS", "ASTRAL.NS",
#     "ATUL.NS", "AUROPHARMA.NS", "AVANTIFEED.NS", "DMART.NS", "AXISBANK.NS", "BEML.NS", "BLS.NS", "BSE.NS", "BAJAJ-AUTO.NS", "BAJFINANCE.NS",
#     "BAJAJFINSV.NS", "BAJAJHLDNG.NS", "BALAMINES.NS", "BALKRISIND.NS", "BALRAMCHIN.NS", "BANDHANBNK.NS", "BANKBARODA.NS", "BANKINDIA.NS",
#     "MAHABANK.NS", "BATAINDIA.NS", "BAYERCROP.NS", "BERGEPAINT.NS", "BDL.NS", "BEL.NS", "BHARATFORG.NS", "BHEL.NS", "BPCL.NS", "BHARTIARTL.NS",
#     "BIKAJI.NS", "BIOCON.NS", "BIRLACORPN.NS", "BSOFT.NS", "BLUEDART.NS", "BLUESTARCO.NS", "BBTC.NS", "BORORENEW.NS", "BOSCHLTD.NS", "BRIGADE.NS",
#     "BRITANNIA.NS", "MAPMYINDIA.NS", "CCL.NS", "CESC.NS", "CGPOWER.NS", "CIEINDIA.NS", "CRISIL.NS", "CSBBANK.NS", "CAMPUS.NS", "CANFINHOME.NS",
#     "CANBK.NS", "CAPLIPOINT.NS", "CGCL.NS", "CARBORUNIV.NS", "CASTROLIND.NS", "CEATLTD.NS", "CELLO.NS", "CENTRALBK.NS", "CDSL.NS", "CENTURYPLY.NS",
#     "CENTURYTEX.NS", "CERA.NS", "CHALET.NS", "CHAMBLFERT.NS", "CHEMPLASTS.NS", "CHENNPETRO.NS", "CHOLAHLDNG.NS", "CHOLAFIN.NS", "CIPLA.NS", "CUB.NS",
#     "CLEAN.NS", "COALINDIA.NS", "COCHINSHIP.NS", "COFORGE.NS", "COLPAL.NS", "CAMS.NS", "CONCORDBIO.NS", "CONCOR.NS", "COROMANDEL.NS", "CRAFTSMAN.NS",
#     "CREDITACC.NS", "CROMPTON.NS", "CUMMINSIND.NS", "CYIENT.NS", "DCMSHRIRAM.NS", "DLF.NS", "DOMS.NS", "DABUR.NS", "DALBHARAT.NS", "DATAPATTNS.NS",
#     "DEEPAKFERT.NS", "DEEPAKNTR.NS", "DELHIVERY.NS", "DEVYANI.NS", "DIVISLAB.NS", "DIXON.NS", "LALPATHLAB.NS", "DRREDDY.NS", "DUMMYSANOF.NS",
#     "EIDPARRY.NS", "EIHOTEL.NS", "EPL.NS", "EASEMYTRIP.NS", "EICHERMOT.NS", "ELECON.NS", "ELGIEQUIP.NS", "EMAMILTD.NS", "ENDURANCE.NS", "ENGINERSIN.NS",
#     "EQUITASBNK.NS", "ERIS.NS", "ESCORTS.NS", "EXIDEIND.NS", "FDC.NS", "NYKAA.NS", "FEDERALBNK.NS", "FACT.NS", "FINEORG.NS", "FINCABLES.NS", "FINPIPE.NS",
#     "FSL.NS", "FIVESTAR.NS", "FORTIS.NS", "GAIL.NS", "GMMPFAUDLR.NS", "GMRINFRA.NS", "GRSE.NS", "GICRE.NS", "GILLETTE.NS", "GLAND.NS", "GLAXO.NS", "GLS.NS",
#     "GLENMARK.NS", "MEDANTA.NS", "GPIL.NS", "GODFRYPHLP.NS", "GODREJCP.NS", "GODREJIND.NS", "GODREJPROP.NS", "GRANULES.NS", "GRAPHITE.NS", "GRASIM.NS",
#     "GESHIP.NS", "GRINDWELL.NS", "GAEL.NS", "FLUOROCHEM.NS", "GUJGASLTD.NS", "GMDCLTD.NS", "GNFC.NS", "GPPL.NS", "GSFC.NS", "GSPL.NS", "HEG.NS", "HBLPOWER.NS",
#     "HCLTECH.NS", "HDFCAMC.NS", "HDFCBANK.NS", "HDFCLIFE.NS", "HFCL.NS", "HAPPSTMNDS.NS", "HAPPYFORGE.NS", "HAVELLS.NS", "HEROMOTOCO.NS", "HSCL.NS", "HINDALCO.NS",
#     "HAL.NS", "HINDCOPPER.NS", "HINDPETRO.NS", "HINDUNILVR.NS", "HINDZINC.NS", "POWERINDIA.NS", "HOMEFIRST.NS", "HONASA.NS", "HONAUT.NS", "HUDCO.NS", "ICICIBANK.NS",
#     "ICICIGI.NS", "ICICIPRULI.NS", "ISEC.NS", "IDBI.NS", "IDFCFIRSTB.NS", "IDFC.NS", "IIFL.NS", "IRB.NS", "IRCON.NS", "ITC.NS", "ITI.NS", "INDIACEM.NS", "IBULHSGFIN.NS",
#     "INDIAMART.NS", "INDIANB.NS", "IEX.NS", "INDHOTEL.NS", "IOC.NS", "IOB.NS", "IRCTC.NS", "IRFC.NS", "INDIGOPNTS.NS", "IGL.NS", "INDUSTOWER.NS", "INDUSINDBK.NS",
#     "NAUKRI.NS", "INFY.NS", "INOXWIND.NS", "INTELLECT.NS", "INDIGO.NS", "IPCALAB.NS", "JBCHEPHARM.NS", "JKCEMENT.NS", "JBMA.NS", "JKLAKSHMI.NS", "JKPAPER.NS",
#     "JMFINANCIL.NS", "JSWENERGY.NS", "JSWINFRA.NS", "JSWSTEEL.NS", "JAIBALAJI.NS", "J&KBANK.NS", "JINDALSAW.NS", "JSL.NS", "JINDALSTEL.NS", "JIOFIN.NS",
#     "JUBLFOOD.NS", "JUBLINGREA.NS", "JUBLPHARMA.NS", "JWL.NS", "JUSTDIAL.NS", "JYOTHYLAB.NS", "KPRMILL.NS", "KEI.NS", "KNRCON.NS", "KPITTECH.NS", "KRBL.NS",
#     "KSB.NS", "KAJARIACER.NS", "KPIL.NS", "KALYANKJIL.NS", "KANSAINER.NS", "KARURVYSYA.NS", "KAYNES.NS", "KEC.NS", "KFINTECH.NS", "KOTAKBANK.NS", "KIMS.NS",
#     "LTF.NS", "LTTS.NS", "LICHSGFIN.NS", "LTIM.NS", "LT.NS", "LATENTVIEW.NS", "LAURUSLABS.NS", "LXCHEM.NS", "LEMONTREE.NS", "LICI.NS", "LINDEINDIA.NS",
#     "LLOYDSME.NS", "LUPIN.NS", "MMTC.NS", "MRF.NS", "MTARTECH.NS", "LODHA.NS", "MGL.NS", "MAHSEAMLES.NS", "M&MFIN.NS", "M&M.NS", "MHRIL.NS", "MAHLIFE.NS",
#     "MANAPPURAM.NS", "MRPL.NS", "MANKIND.NS", "MARICO.NS", "MARUTI.NS", "MASTEK.NS", "MFSL.NS", "MAXHEALTH.NS", "MAZDOCK.NS", "MEDPLUS.NS", "METROBRAND.NS",
#     "METROPOLIS.NS", "MINDACORP.NS", "MSUMI.NS", "MOTILALOFS.NS", "MPHASIS.NS", "MCX.NS", "MUTHOOTFIN.NS", "NATCOPHARM.NS", "NBCC.NS", "NCC.NS", "NHPC.NS",
#     "NLCINDIA.NS", "NMDC.NS", "NSLNISP.NS", "NTPC.NS", "NH.NS", "NATIONALUM.NS", "NAVINFLUOR.NS", "NESTLEIND.NS", "NETWORK18.NS", "NAM-INDIA.NS", "NUVAMA.NS",
#     "NUVOCO.NS", "OBEROIRLTY.NS", "ONGC.NS", "OIL.NS", "OLECTRA.NS", "PAYTM.NS", "OFSS.NS", "POLICYBZR.NS", "PCBL.NS", "PIIND.NS", "PNBHOUSING.NS",
#     "PNCINFRA.NS", "PVRINOX.NS", "PAGEIND.NS", "PATANJALI.NS", "PERSISTENT.NS", "PETRONET.NS", "PHOENIXLTD.NS", "PIDILITIND.NS", "PEL.NS", "PPLPHARMA.NS",
#     "POLYMED.NS", "POLYCAB.NS", "POONAWALLA.NS", "PFC.NS", "POWERGRID.NS", "PRAJIND.NS", "PRESTIGE.NS", "PRINCEPIPE.NS", "PRSMJOHNSN.NS", "PGHH.NS",
#     "PNB.NS", "QUESS.NS", "RRKABEL.NS", "RBLBANK.NS", "RECLTD.NS", "RHIM.NS", "RITES.NS", "RADICO.NS", "RVNL.NS", "RAILTEL.NS", "RAINBOW.NS",
#     "RAJESHEXPO.NS", "RKFORGE.NS", "RCF.NS", "RATNAMANI.NS", "RTNINDIA.NS", "RAYMOND.NS", "REDINGTON.NS", "RELIANCE.NS", "RBA.NS", "ROUTE.NS",
#     "SBFC.NS", "SBICARD.NS", "SBILIFE.NS", "SJVN.NS", "SKFINDIA.NS", "SRF.NS", "SAFARI.NS", "MOTHERSON.NS", "SANOFI.NS", "SAPPHIRE.NS", "SAREGAMA.NS",
#     "SCHAEFFLER.NS", "SCHNEIDER.NS", "SHREECEM.NS", "RENUKA.NS", "SHRIRAMFIN.NS", "SHYAMMETL.NS", "SIEMENS.NS", "SIGNATURE.NS", "SOBHA.NS", "SOLARINDS.NS",
#     "SONACOMS.NS", "SONATSOFTW.NS", "STARHEALTH.NS", "SBIN.NS", "SAIL.NS", "SWSOLAR.NS", "STLTECH.NS", "SUMICHEM.NS", "SPARC.NS", "SUNPHARMA.NS",
#     "SUNTV.NS", "SUNDARMFIN.NS", "SUNDRMFAST.NS", "SUNTECK.NS", "SUPREMEIND.NS", "SUVENPHAR.NS", "SUZLON.NS", "SWANENERGY.NS", "SYNGENE.NS", "SYRMA.NS",
#     "TV18BRDCST.NS", "TVSMOTOR.NS", "TVSSCS.NS", "TMB.NS", "TANLA.NS", "TATACHEM.NS", "TATACOMM.NS", "TCS.NS", "TATACONSUM.NS", "TATAELXSI.NS",
#     "TATAINVEST.NS", "TATAMTRDVR.NS", "TATAMOTORS.NS", "TATAPOWER.NS", "TATASTEEL.NS", "TATATECH.NS", "TTML.NS", "TECHM.NS", "TEJASNET.NS", "NIACL.NS",
#     "RAMCOCEM.NS", "THERMAX.NS", "TIMKEN.NS", "TITAGARH.NS", "TITAN.NS", "TORNTPHARM.NS", "TORNTPOWER.NS", "TRENT.NS", "TRIDENT.NS", "TRIVENI.NS",
#     "TRITURBINE.NS", "TIINDIA.NS", "UCOBANK.NS", "UNOMINDA.NS", "UPL.NS", "UTIAMC.NS", "UJJIVANSFB.NS", "ULTRACEMCO.NS", "UNIONBANK.NS", "UBL.NS",
#     "UNITDSPR.NS", "USHAMART.NS", "VGUARD.NS", "VIPIND.NS", "VAIBHAVGBL.NS", "VTL.NS", "VARROC.NS", "VBL.NS", "MANYAVAR.NS", "VEDL.NS", "VIJAYA.NS",
#     "IDEA.NS", "VOLTAS.NS", "WELCORP.NS", "WELSPUNLIV.NS", "WESTLIFE.NS", "WHIRLPOOL.NS", "WIPRO.NS", "YESBANK.NS", "ZFCVINDIA.NS", "ZEEL.NS", "ZENSARTECH.NS",
#     "ZOMATO.NS", "ZYDUSLIFE.NS", "ECLERX.NS"
# ]
def getTickers():
    return [
    "360ONE.NS", "3MINDIA.NS", "ABB.NS", "ACC.NS", "AIAENG.NS", "APLAPOLLO.NS", "AUBANK.NS", "AARTIIND.NS", "AAVAS.NS", "ABBOTINDIA.NS",
    "ACE.NS", "ADANIENSOL.NS", "ADANIENT.NS", "ADANIGREEN.NS", "ADANIPORTS.NS", "ADANIPOWER.NS", "ATGL.NS", "AWL.NS", "ABCAPITAL.NS",
    "ABFRL.NS", "AEGISLOG.NS", "AETHER.NS", "AFFLE.NS", "AJANTPHARM.NS", "APLLTD.NS", "ALKEM.NS", "ALKYLAMINE.NS", "ALLCARGO.NS",
    "ALOKINDS.NS", "ARE&M.NS", "AMBER.NS", "AMBUJACEM.NS", "ANANDRATHI.NS", "ANGELONE.NS", "ANURAS.NS", "APARINDS.NS", "APOLLOHOSP.NS",
    "APOLLOTYRE.NS", "APTUS.NS", "ACI.NS", "ASAHIINDIA.NS", "ASHOKLEY.NS", "ASIANPAINT.NS", "ASTERDM.NS", "ASTRAZEN.NS", "ASTRAL.NS",
    "ATUL.NS", "AUROPHARMA.NS", "AVANTIFEED.NS", "DMART.NS", "AXISBANK.NS", "BEML.NS", "BLS.NS", "BSE.NS", "BAJAJ-AUTO.NS", "BAJFINANCE.NS",
    "BAJAJFINSV.NS", "BAJAJHLDNG.NS", "BALAMINES.NS", "BALKRISIND.NS", "BALRAMCHIN.NS", "BANDHANBNK.NS", "BANKBARODA.NS", "BANKINDIA.NS",
    "MAHABANK.NS", "BATAINDIA.NS", "BAYERCROP.NS", "BERGEPAINT.NS", "BDL.NS", "BEL.NS", "BHARATFORG.NS", "BHEL.NS", "BPCL.NS", "BHARTIARTL.NS",
    "BIKAJI.NS", "BIOCON.NS", "BIRLACORPN.NS", "BSOFT.NS", "BLUEDART.NS", "BLUESTARCO.NS", "BBTC.NS", "BORORENEW.NS", "BOSCHLTD.NS", "BRIGADE.NS",
    "BRITANNIA.NS", "MAPMYINDIA.NS", "CCL.NS", "CESC.NS", "CGPOWER.NS", "CIEINDIA.NS", "CRISIL.NS", "CSBBANK.NS", "CAMPUS.NS", "CANFINHOME.NS",
    "CANBK.NS", "CAPLIPOINT.NS", "CGCL.NS", "CARBORUNIV.NS", "CASTROLIND.NS", "CEATLTD.NS", "CELLO.NS", "CENTRALBK.NS", "CDSL.NS", "CENTURYPLY.NS",
    "CENTURYTEX.NS", "CERA.NS", "CHALET.NS", "CHAMBLFERT.NS", "CHEMPLASTS.NS", "CHENNPETRO.NS", "CHOLAHLDNG.NS", "CHOLAFIN.NS", "CIPLA.NS", "CUB.NS",
    "CLEAN.NS", "COALINDIA.NS", "COCHINSHIP.NS", "COFORGE.NS", "COLPAL.NS", "CAMS.NS", "CONCORDBIO.NS", "CONCOR.NS", "COROMANDEL.NS", "CRAFTSMAN.NS",
    "CREDITACC.NS", "CROMPTON.NS", "CUMMINSIND.NS", "CYIENT.NS", "DCMSHRIRAM.NS", "DLF.NS", "DOMS.NS", "DABUR.NS", "DALBHARAT.NS", "DATAPATTNS.NS",
    "DEEPAKFERT.NS",
]
# Function to calculate Sharpe ratio
def sharpe_ratio(returns, risk_free_rate=0.05 / 252):
    return (np.mean(returns) - risk_free_rate) / np.std(returns)
    
def analyze_stock(df):
    # Ensure df is a copy of the original DataFrame to avoid the warning
    df = df.copy()

    # Calculate the Exponential Moving Averages (EMA)
    df['EMA100'] = df['Close'].ewm(span=100).mean()
    df['EMA200'] = df['Close'].ewm(span=200).mean()

    # Calculate various stock metrics
    one_year_return = (df['Close'].iloc[-1] / df['Close'].iloc[-252] - 1) * 100
    high_52_week = df['Close'].iloc[-252:].max()
    within_10_pct_high = df['Close'].iloc[-1] >= high_52_week * 0.6

    # Calculate the percentage of up days in the last six months
    six_month_data = df['Close'].iloc[-126:]
    up_days = (six_month_data.pct_change() > 0).sum()
    up_days_pct = up_days / len(six_month_data) * 100

    # Calculate returns over 6, 3, and 1 month periods
    return_6m = (df['Close'].iloc[-1] / df['Close'].iloc[-126] - 1) * 100
    return_3m = (df['Close'].iloc[-1] / df['Close'].iloc[-63] - 1) * 100
    return_1m = (df['Close'].iloc[-1] / df['Close'].iloc[-21] - 1) * 100

    # Calculate returns for 3, 6, and 12 months to be used for Sharpe ratio
    returns_3m = df['Close'].iloc[-63:].pct_change().dropna()
    returns_6m = df['Close'].iloc[-126:].pct_change().dropna()
    returns_12m = df['Close'].iloc[-252:].pct_change().dropna()

    # Calculate Sharpe ratios
    sharpe_3m = sharpe_ratio(returns_3m)
    sharpe_6m = sharpe_ratio(returns_6m)
    sharpe_12m = sharpe_ratio(returns_12m)
    avg_sharpe = (sharpe_3m + sharpe_6m + sharpe_12m) / 3

    # Return the analyzed stock metrics
    return {
        'EMA100': df['EMA100'].iloc[-1],
        'EMA200': df['EMA200'].iloc[-1],
        'One_Year_Return': one_year_return,
        'Within_10_Pct_High': within_10_pct_high,
        'Up_Days_Pct': up_days_pct,
        'Return_6M': return_6m,
        'Return_3M': return_3m,
        'Return_1M': return_1m,
        'Sharpe_3M': sharpe_3m,
        'Sharpe_6M': sharpe_6m,
        'Sharpe_12M': sharpe_12m,
        'Avg_Sharpe': avg_sharpe,
    }

# Function to check filtering criteria
def meets_criteria(stock_data, min_return):
    return (stock_data['EMA100'] >= stock_data['EMA200'] and
            stock_data['One_Year_Return'] >= min_return and
            stock_data['Within_10_Pct_High'] and
            stock_data['Up_Days_Pct'] > 50)

def download_and_save_data(tickers, start_date, end_date):
    logging.info("In download and save data")

    all_data = []
    base_dir = "/Users/kartikaypandey/Documents/django_projects/stockproject/"
    file_path = f"{base_dir}{datetime.now().strftime('%Y-%m-%d')}.csv"
    # Check if the file already exists
    if os.path.exists(file_path):
        logging.info("Using existing file for data")
        combined_df = pd.read_csv(file_path)
    else:
        # Download data if the file does not exist
        for ticker in tickers:
            try:
                logging.info(f"Data downloaded for {ticker}")
                df = yf.download(ticker, start=start_date, end=end_date)
                df['Ticker'] = ticker
                all_data.append(df)
            except Exception as e:
                logging.info(f"Error downloading data for {ticker}: {e}")
        
        if all_data:
            combined_df = pd.concat(all_data)
            combined_df.to_csv(file_path, index=False)
            logging.info(f"Data downloaded and saved to {file_path}")
        else:
            logging.info("No data downloaded.")
            return None

    return combined_df

def process_tickers(tickers, start_date, end_date,min_return=6.5):
    logging.info("Processing Tickers ..")

    summary = []

    # Use the data from the saved file or downloaded data
    combined_df = download_and_save_data(tickers, start_date, end_date)
    
    if combined_df is None:
        return summary

    # Process each ticker's data
    for ticker in tickers:
        try:
            df = combined_df[combined_df['Ticker'] == ticker]
            if len(df) > 0:
                stock_data = analyze_stock(df)
                if meets_criteria(stock_data, min_return):
                    summary.append({
                        'Ticker': ticker,
                        'Return_6M': stock_data['Return_6M'],
                        'Return_3M': stock_data['Return_3M'],
                        'Return_1M': stock_data['Return_1M'],
                        'Sharpe_3M': stock_data['Sharpe_3M'],
                        'Sharpe_6M': stock_data['Sharpe_6M'],
                        'Sharpe_12M': stock_data['Sharpe_12M'],
                        'Avg_Sharpe': stock_data['Avg_Sharpe'],
                    })
        except Exception as e:
            logging.info(f"Error analyzing {ticker}: {e}")

    return summary


def rank_and_sort_summary(summary, n):
    if not summary:
        logging.info("No stocks meet the criteria.")
        return pd.DataFrame()  # Return an empty DataFrame or handle as needed
    
    df_summary = pd.DataFrame(summary)
    
    df_summary['Return_6M'] = df_summary['Return_6M'].round(1)
    df_summary['Return_3M'] = df_summary['Return_3M'].round(1)
    df_summary['Return_1M'] = df_summary['Return_1M'].round(1)
    df_summary['Avg_Sharpe'] = df_summary['Avg_Sharpe'].round(2)

    df_summary['Sharpe_3M'] = df_summary['Sharpe_3M'].round(1)
    df_summary['Sharpe_6M'] = df_summary['Sharpe_6M'].round(1)
    df_summary['Sharpe_12M'] = df_summary['Sharpe_12M'].round(1)

    df_summary['Rank_6M'] = df_summary['Return_6M'].rank(ascending=False)
    df_summary['Rank_3M'] = df_summary['Return_3M'].rank(ascending=False)
    df_summary['Rank_1M'] = df_summary['Return_1M'].rank(ascending=False)
    df_summary['Rank_Sharpe_3M'] = df_summary['Sharpe_3M'].rank(ascending=False)
    df_summary['Rank_Sharpe_6M'] = df_summary['Sharpe_6M'].rank(ascending=False)
    df_summary['Rank_Sharpe_12M'] = df_summary['Sharpe_12M'].rank(ascending=False)
    df_summary['Rank_Sharpe'] = df_summary['Avg_Sharpe'].rank(ascending=False)

    df_summary['Final_Rank'] = (df_summary['Rank_6M'] + df_summary['Rank_3M'] +
                                 df_summary['Rank_1M'] + df_summary['Rank_Sharpe'])

    df_summary_sorted = df_summary.sort_values('Rank_Sharpe').head(n)
    df_summary_sorted['Position'] = np.arange(1, len(df_summary_sorted) + 1)

    return df_summary_sorted

def delete_old_files(base_dir, current_date):
    # List all files in the directory
    for file_name in os.listdir(base_dir):
        file_path = os.path.join(base_dir, file_name)
        if file_name.endswith('.csv') and current_date not in file_name:
            try:
                os.remove(file_path)
                logging.info(f"Deleted old file: {file_path}")
            except Exception as e:
                logging.error(f"Failed to delete file {file_path}: {e}")
                
#  * * * * 
# /Users/kartikaypandey/Documents/django_projects/stockproject/venv/bin/python3 
# /Users/kartikaypandey/Documents/django_projects/stockproject/scripts/download_stock_data.py >> 
# /Users/kartikaypandey/Documents/django_projects/stockproject/scripts/cron_job.log 2>&1
def download_stock_data():
    logging.info("Started downloading stock data.")
    end_date = datetime.today()
    start_date = end_date - timedelta(days=2*365)  # 2 years of data
    logging.info("Getting Tickers ..")
    tickers = getTickers()
    summary = process_tickers(tickers, start_date, end_date)
    sorted_data = rank_and_sort_summary(summary, 100)

    data = sorted_data.to_dict(orient='records')
    # Save the data to a CSV file with today's date
    base_dir = "/Users/kartikaypandey/Documents/django_projects/stockproject/"
    current_date = datetime.now().strftime('%Y-%m-%d')
    file_path = f"{base_dir}top50_{current_date}.csv"
    
    try:
        sorted_data.to_csv(file_path)
        logging.info(f"Data saved to {file_path}")
    except Exception as e:
        logging.error(f"Failed to save data to CSV: {e}")

    # After saving, delete older files
    delete_old_files(base_dir, current_date)

if __name__ == "__main__":
    download_stock_data()


