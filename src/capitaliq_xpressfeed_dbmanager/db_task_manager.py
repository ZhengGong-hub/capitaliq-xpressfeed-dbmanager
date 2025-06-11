from .logger import get_logger
from .base_database import BaseDatabase
import pandas as pd

logger = get_logger(__name__)

class TaskManagerRepository:
    """Repository for handling task operations with api."""

    def __init__(self, database: BaseDatabase):
        """Initialize repository with database connection.

        Args:
            database: Database instance for data access
        """
        self.database = database

    def test_connection_query(self) -> pd.DataFrame:
        """Test the connection to the database.

        Returns:
            pd.DataFrame: A dataframe with the company ID.
        """
        return self.database.query_all("SELECT * from ciqcompany limit 10;")

    def query_global_market_cap(self, asofdate: str, mktcap_thres: float, country: str = "US", allow_fuzzy: bool = False) -> pd.DataFrame:
        """Query the global market cap that is above the threshold and at a given date.

        we do not really need the fuzzy, as the marketcap is pretty dense over vacations and holidays

        Args:
            asofdate: The date to query the market cap for
            mktcap_thres: The market cap threshold (in million USD)
            country: The country code to filter companies (default: "US")
            allow_fuzzy: If True, look for data within 5 days of asofdate if exact date not available
        Returns:
            pd.DataFrame: A dataframe with the company ID and market cap
        """
        # check asofdate is a str
        if not isinstance(asofdate, str):
            raise ValueError("asofdate must be a string")

        if country == "Global":
            all_countries = True
        else:
            all_countries = False

        # Common SELECT fields and table joins for both scenarios
        query = """
            SELECT 
                ciqmarketcap.companyid,
                ciqmarketcap.marketcap,
                ciqmarketcap.pricingdate,
                round(ciqmarketcap.marketcap / ciqexchangerate.priceclose, 2) as usdmarketcap,
                ciqcompany.companyname,
                ciqtradingitem.tickersymbol,
                ciqcurrency.isocode as currency,
                ciqexchange.exchangesymbol as exchange,
                ciqcountrygeo.isocountry2 as country
            FROM
                ciqmarketcap
            JOIN
                ciqcompany ON ciqmarketcap.companyID = ciqcompany.companyID
            JOIN
                ciqsecurity ON ciqmarketcap.companyID = ciqsecurity.companyID
            JOIN 
                ciqtradingitem on ciqsecurity.securityid = ciqtradingitem.securityid
            JOIN 
                ciqexchangerate on ciqtradingitem.currencyid = ciqexchangerate.currencyid
            JOIN
                ciqcurrency on ciqtradingitem.currencyid = ciqcurrency.currencyid
            JOIN
                ciqexchange on ciqtradingitem.exchangeid = ciqexchange.exchangeid
            JOIN 
                ciqcountrygeo on ciqcompany.countryid = ciqcountrygeo.countryid
            WHERE
        """

        # Date conditions differ based on allow_fuzzy
        if allow_fuzzy:
            query += f"""
                ciqmarketcap.pricingdate BETWEEN DATE('{asofdate}') - INTERVAL '3 days' AND '{asofdate}'
            """
        else:
            query += f"""
                ciqmarketcap.pricingdate = '{asofdate}'
            """

        # add country filter if not all countries
        if all_countries:
            pass
        else:
            query += f"""
                AND 
                    ciqcountrygeo.isocountry2 = '{country}'
            """

        # Common WHERE conditions for both scenarios
        query += f"""
            AND
                ciqexchangerate.pricedate = '{asofdate}'
            AND
                ciqexchangerate.latestsnapflag = 1
            AND
                ciqmarketcap.marketcap / ciqexchangerate.priceclose >= {mktcap_thres}
            AND
                ciqcompany.companytypeid in (4, 5)
            AND 
                ciqsecurity.primaryflag = 1
            AND 
                ciqtradingitem.primaryflag = 1
            ORDER BY
                ciqmarketcap.pricingdate DESC, usdmarketcap DESC
        """

        return self.database.query_all(query)
    

    def get_security_info(self, ticker: str, country: str) -> pd.DataFrame:
        """Get company, security, and trading item information for a ticker
        
        Args:
            ticker (str): Stock ticker symbol
            
        Returns:
            pd.DataFrame: Security information
        """
        sql = f"""
        select t.*, s.*, c.*, upper(cg.isocountry2) as countrycode
        from ciqtradingitem t
        join ciqsecurity s on t.securityid = s.securityid
        join ciqcompany c on s.companyid = c.companyid
        join ciqcountrygeo cg on c.countryid = cg.countryid
        where t.tickersymbol = '{ticker}'
        and cg.isocountry2 = '{country}'
        and
        t.primaryflag = 1
        and
        s.primaryflag = 1
        """
        return self.database.query_all(sql)

    def get_metadata_info(self, ticker: str, country: str) -> int:
        """Get company id for a given ticker
        
        Args:
            ticker (str): Stock ticker symbol
            
        Returns:
            int: Company id
        """
        _df = self.get_security_info(ticker, country)
        if len(_df) > 1 or len(_df) == 0:
            raise Exception(f"Multiple or no security found for {ticker}")
        else:
            return _df.iloc[0]


    def get_company_transcriptsid(self, companyid: int, last_refresh_day: str) -> pd.DataFrame:
        """
        Get the transcripts of given companies id from start date to end date
        Args:
            companyid (int): company id e.g. 11686323
        
        Returns:
            pd.DataFrame: Transcript metadata
        """
        datestart = pd.to_datetime(last_refresh_day).strftime("%Y-%m-%d")

        sql = f"""
            SELECT t.transcriptId, t.transcriptCreationDateUTC, ete.objectId companyid, t.keyDevId, t.transcriptCollectionTypeId,
            e.mostImportantDateUTC as EarningsCallDateUTC, e.announcedDateUTC,
            eb.fiscalyear, eb.fiscalquarter
            FROM targetskma.ciqTranscript t
            JOIN targetskma.ciqEvent e ON e.keyDevId = t.keyDevId
            JOIN targetskma.ciqEventToObjectToEventType ete ON ete.keyDevId = t.keyDevId
            JOIN targetskma.ciqEventType et ON et.keyDevEventTypeId = ete.keyDevEventTypeId
            JOIN targetskma.ciqeventcallbasicinfo eb on eb.keyDevId = t.keyDevId
            WHERE et.keyDevEventTypeId='48' --Earnings Calls
            AND ete.objectId = {companyid}   
            AND t.transcriptCreationDateUTC > '{datestart}'
            ORDER BY e.mostImportantDateUTC asc;
            """

        df = self.database.query_all(sql)
        et_ref = df.sort_values(['keydevid', 'transcriptcreationdateutc']).drop_duplicates('keydevid', keep='last') # get the max id, that is with latest transcriptcreationdateutc
        return et_ref

    def get_latest_transcriptid(self, companyids: list[int], last_refresh_day: str) -> int:
        # last refresh day should be today - 1 year
        last_refresh_day = (pd.Timestamp.now() - pd.Timedelta(days=365)).strftime("%Y-%m-%d")
        df = self.get_company_transcriptsid(companyids, last_refresh_day)
        return df.tail(1)['transcriptid'].values[0]

    def get_transcript(self, ls_transcript_ids):
        """
        Get transcript given a list of transcript ids
        Args:
            ls_transcript_ids (list): list of transcriptid e.g. [2228812, ]
        Returns:
            pd.DataFrame: Transcript data
        """
        sql = f"""
        select tc.transcriptComponentId, tc.transcriptId, tc.componentOrder, tc.transcriptComponentTypeId, 
        tc.transcriptPersonId, CAST(tc.componentText AS TEXT) AS componentText, tct.transcriptComponentTypeName, tp.transcriptPersonName, tst.speakerTypeName, pb.title
        from targetskma.ciqTranscriptComponent tc
        LEFT JOIN targetskma.ciqTranscriptPerson tp on tc.transcriptPersonId = tp.transcriptPersonId
        LEFT JOIN targetskma.ciqTranscriptSpeakerType tst on tst.speakerTypeId = tp.speakerTypeId
        LEFT JOIN targetskma.ciqProfessional pb on pb.proId= tp.proId
        LEFT JOIN targetskma.ciqTranscriptComponentType tct on tc.transcriptComponentTypeId = tct.transcriptComponentTypeId
        where transcriptId in ({', '.join([str(id) for id in ls_transcript_ids])}) 
        order by transcriptId, componentOrder
        """

        return self.database.query_all(sql)


    def get_act_q_ref_co(self, ls_ids, dataitemids, startdate):
        datestart = pd.to_datetime(startdate).strftime("%Y-%m-%d")

        sql = f"""
            select 
            EP.periodTypeId
            , EP.periodenddate
            , EP.fiscalyear
            , EP.fiscalquarter
            , ED.dataitemid
            , ED.currencyId
            , ED.dataItemValue
            , ED.effectiveDate
            , ED.toDate
            , ED.estimatescaleid
            , di.dataitemname

            from ciqEstimatePeriod EP
            --- link the core estimate table to data table
            --------------------------------------------------------------
            join ciqEstimateConsensus EC 
            on EC.estimatePeriodId = EP.estimatePeriodId
            join ciqEstimateNumericData ED
            on ED.estimateConsensusId = EC.estimateConsensusId
            join ciqdataitem di on di.dataitemid = ED.dataitemid
            --------------------------------------------------------------
            where EP.companyId IN ({', '.join([str(id) for id in ls_ids])})
            and EP.periodTypeId = 2 -- Quarter 
            and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
            and EP.periodenddate > '{datestart}'
            and ED.toDate > '2030-01-01'

            order by 4
        """

        return self.database.query_all(sql)

    def get_historical_fundamental(self, ls_ids, ls_dataitemid, periodtypeid = [1, 2], startyear = 2007):

        """
        @author: zheng
        """    
        startdate = pd.to_datetime(f"{startyear}-01-01").strftime("%Y-%m-%d")

        sql = f"""
                SELECT 
                fp.companyId, 
                fi.periodEndDate,
                fi.filingDate,
                fi.formtype,
                fi.currencyid,
                fp.periodTypeId, 
                fp.calendarQuarter, 
                fp.calendarYear,
                fd.dataItemId,
                fd.dataItemValue,
                fid.instanceDate,
                di.dataitemname
                
                FROM ciqFinPeriod fp 
                join ciqFinInstance fi on fi.financialPeriodId = fp.financialPeriodId 
                join ciqFinInstanceDate fid on fid.financialInstanceId = fi.financialInstanceId
                join ciqFinInstanceToCollection ic on ic.financialInstanceId = fi.financialInstanceId 
                join ciqFinCollectionData fd on fd.financialCollectionId = ic.financialCollectionId 
                join ciqdataitem di on di.dataitemid = fd.dataItemId
                
                WHERE fd.dataItemId in ({', '.join([str(id) for id in ls_dataitemid])})  
                AND    fp.companyId in ({', '.join([str(id) for id in ls_ids])})
                AND    fp.calendarYear >= '{startyear}'
                AND    fp.periodTypeId in ({', '.join([str(id) for id in periodtypeid])}) --quarterly 
                AND  fi.periodEndDate >= '{startdate}'
                
            """
        
        df = self.database.query_all(sql)   

        # round the dataitemvalue to 2 decimal places
        df['dataitemvalue'] = df['dataitemvalue'].astype(float).round(2)
        return df

    def get_key_fundamentals(self, companyids: list[int], dataitemids: list[int], traling_x_years: int = 5) -> pd.DataFrame:
        # today - trailing x years
        startdate = (pd.Timestamp.now() - pd.Timedelta(days=365 * traling_x_years)).strftime("%Y-%m-%d")
        if dataitemids is None:
            dataitemids = [100186, 100284, 100179]
        _df = self.get_act_q_ref_co(companyids, dataitemids, startdate)

        # pivot so that the periodenddate is the index
        _df = _df.pivot(index='periodenddate', columns='dataitemid', values='dataitemvalue').reset_index()
        _df.rename(columns={100186: 'Revenue', 100284: 'EPS', 100179: 'Normalized EPS'}, inplace=True)
        _df['Revenue'] = _df['Revenue'].astype(float).round(3)
        _df['EPS'] = _df['EPS'].astype(float).round(3)
        _df['Normalized EPS'] = _df['Normalized EPS'].astype(float).round(3)
        _df['periodenddate'] = pd.to_datetime(_df['periodenddate'])
        return _df

    def get_past_price(self, companyid: int, traling_x_years: int = 5) -> pd.DataFrame:

        # enddate should be today
        enddate = pd.Timestamp.now().strftime("%Y-%m-%d")
        # startdate should be today - 1 year
        startdate = (pd.Timestamp.now() - pd.Timedelta(days=365 * traling_x_years)).strftime("%Y-%m-%d")

        sql = f"""
        SELECT 
        c.companyid
        ,ti.tradingItemId
        ,ti.currencyid
        ,mi.priceDate
        ,mi.priceClose
        ,mi.priceOpen
        ,mi.priceHigh
        ,mi.priceLow
        ,mi.volume
        ,mi.vwap

        ,(mi.priceClose*COALESCE(daf.divAdjFactor,1)) divAdjClose
        ,COALESCE(daf.divAdjFactor,1) as divAdjFactor

        FROM ciqCompany c
        JOIN ciqSecurity s on s.companyid = c.companyid
        JOIN ciqTradingItem ti on ti.securityId=s.securityId
        JOIN miadjprice mi on mi.tradingItemId=ti.tradingItemId

        left join ciqPriceEquityDivAdjFactor daf on mi.tradingItemId=daf.tradingItemId
        and daf.fromDate<=mi.priceDate --Find dividend adjustment factor on pricing date
        and (daf.toDate is null or daf.toDate>=mi.priceDate)

        WHERE c.companyId = {companyid}
        AND s.primaryflag=1 -- empirically makes sense to have these primary flag, lost about 0.03% data
        AND ti.primaryflag=1
        AND mi.priceDate >= '{startdate}'
        AND mi.priceDate <= '{enddate}'
        ORDER BY mi.priceDate asc;
        """
        _df = self.database.query_all(sql)[['pricedate', 'priceclose', 'priceopen', 'pricehigh', 'pricelow', 'volume', 'vwap', 'divadjclose', 'divadjfactor']]
        # for price, should keep just two digits
        _df['priceclose'] = _df['priceclose'].astype(float).round(2)
        _df['priceopen'] = _df['priceopen'].astype(float).round(2)
        _df['pricehigh'] = _df['pricehigh'].astype(float).round(2)
        _df['pricelow'] = _df['pricelow'].astype(float).round(2)
        _df['volume'] = _df['volume'].astype(float).round(2)
        _df['vwap'] = _df['vwap'].astype(float).round(2)
        _df['divadjclose'] = _df['divadjclose'].astype(float).round(2)
        _df['divadjfactor'] = _df['divadjfactor'].astype(float).round(4)
        _df['pricedate'] = pd.to_datetime(_df['pricedate'])
        return _df
    
    def get_past_priceclose(self, companyid: int, traling_x_years: int = 5) -> pd.DataFrame:
        """
        Get past price close for a given company id
        Args:
            companyid (int): company id
            traling_x_years (int): trailing x years
        Returns:
            pd.DataFrame: past price close
                pricedate (pd.Timestamp), priceclose, follow ascending order
        """
        _df = self.get_past_price(companyid, traling_x_years)
        return _df[['pricedate', 'priceclose']].sort_values(by='pricedate', ascending=True)

    def get_dataitem_info(self, dataitemids: list[int] = None, all: bool = False) -> pd.DataFrame:
        """
        Get dataitem info for a given dataitemid
        Args:
            dataitemids (list): list of dataitemid e.g. [100186, ]
        Returns:
            pd.DataFrame: Dataitem info
        """
        if all:
            sql = f"""
            select * from ciqdataitem
            """
        else:
            sql = f"""
            select * from ciqdataitem where dataitemid in ({', '.join([str(id) for id in dataitemids])})
            """
        return self.database.query_all(sql)
