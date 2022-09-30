#define COMPILEORACLE


using Microsoft.VisualBasic;
using Microsoft.VisualBasic.CompilerServices;
using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Xml.Serialization;
using System.Collections.ObjectModel;

#if COMPILEORACLE
#endif

#pragma warning disable 1591

namespace BasicDAL
{

    public sealed class SimpleKeyedCollection<TKey, TValue> : KeyedCollection<TKey, TValue>
    {
        private readonly Func<TValue, TKey> keySelector;

        public SimpleKeyedCollection(System.Linq.Expressions.Expression<Func<TValue, TKey>> keySelector)
        {
            this.keySelector = keySelector.Compile();
        }

        protected override TKey GetKeyForItem(TValue item)
        {
            return this.keySelector(item);
        }
    }


    #region BasicDALAttributes

    #endregion


    #region ConnectionStringElements

    
    #endregion


    #region DBConfig
    [Serializable]
    public class DbConfig
    {
        public ExecutionResult ExecutionResult = new ExecutionResult();
        private string mServerName;
        private DateTime mNullDateValue = DateTime.Parse("01/01/0001 00:00:00");
        private string mDatabaseName;
        private int mAuthenticationMode;
        private string mUserName = "";
        private string mPassword = "";
        private int mConnectionTimeout = 15;
        private int mPingConnectionTimeout = 15;
        private Providers mProvider = Providers.SqlServer;
        private string mConnectionString = "";
        private string mOptionalConnectionStringParameters;
        private string mParameterNamePrefix = "@";
        private string mParameterNamePrefixE = "@";
        private ParameterStyles mParametersStyle = ParameterStyles.Qualified;
        private bool mSupportSquareBrackets = false;
        private DbConnection mDbConnection;
        private int mCommandTimeout = 600;
        private bool mSupportTopRecords = false;
        private bool mErrorState = false;
        private string strLastError = "";
        private Exception exLasteException = null;
        private int intLastErrorCode = 0;
        private bool boolHandleErrors = true;
        private string strLogFile = "";
        private bool boolLogError = false;
        private bool mSuppressErrorsNotification = false;
        private dynamic mRedirectErrorsNotificationTo = null;
        private bool mOnlyEntityInitialization = false;
        private string mCloneOf = "";
        private string mName = "";
        private DbACL mACL = new DbACL();
        private DbProviderFactory objFactory = null;

        //SQLSERVER
        public string SQLServerConnectionStringBase1 = "Integrated Security=SSPI;Initial Catalog=@DBNAME@;Data Source=@SERVERNAME@;Connection timeout=@CONNECTIONTIMEOUT@;Password=@PASSWORD@";
        public string SQLServerConnectionStringBase2 = "Initial Catalog=@DBNAME@;Data Source=@SERVERNAME@;User ID=@USERNAME@;Password=@PASSWORD@;Connection timeout=@CONNECTIONTIMEOUT@";
        
        //ODBC
        public string ODBCConnectionStringBase1 = "DSN=@SERVERNAME@;Persist Security Info=False;UID=@USERNAME@";
        public string ODBCConnectionStringBase2 = "DSN=@SERVERNAME@;Persist Security Info=True;UID=@USERNAME@;PWD=@PASSWORD@";
        
        // ORACLE
        public string OracleConnectionStringBase1 = "User Id=@USERNAME@;Password=@PASSWORD@;Max Pool Size = @ORACLEMAXPOOLSIZE@;Min Pool Size=@ORACLEMINPOOLSIZE@;Pooling=@ORACLEPOOLING@;Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=@SERVERNAME@)(PORT=@PORT@))(CONNECT_DATA= (SERVER = DEDICATED)(SID=@SERVICENAME@)));";
        public string OracleConnectionStringBase2 = "User Id=@USERNAME@;Password=@PASSWORD@;Max Pool Size = @ORACLEMAXPOOLSIZE@;Min Pool Size=@ORACLEMINPOOLSIZE@;Pooling=@ORACLEPOOLING@;Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=@SERVERNAME@)(PORT=@PORT@))(CONNECT_DATA= (SERVER = DEDICATED)(SERVICE_NAME=@SERVICENAME@)));";
        private string mOracleServiceNameSID = "";
        private bool mOracleUseSID = false;
        private int mOraclePort = 1521;
        private string mOracleVersion = "12.1.0.0";
        private bool mOraclePooling = true;
        private int mOracleMinPoolSize = 1;
        private int mOracleMaxPoolSize = 100;

        //OLEDB
        public string OLEDBConnectionStringBase1 = "";
        public string OLEDBConnectionStringBase2 = "";

        //OLEDB2
        public string OLEDB_DB2ConnectionStringBase1 = "Provider=@OLEDB2PROVIDER@;Password=@PASSWORD@;User ID=@USERNAME@;Data Source=@SERVERNAME@;Initial Catalog=@OLEDB2INITIALCATALOG@;Naming Convention=@OLEDB2NAMINGCONVENTION@;Convert Date Time To Char=@OLEDB2CONVERTDATETIMETOCHAR@;";
        private string mOleDB2Provider = "IBMDA400";
        private string mOleDB2InitialCatalog = "";
        private string mOleDB2NamingConvention = "1";
        private string mOleDB2ConvertDateTimeToChar = "False";

        //DB2iSeries
        public string DB2_iSeriesConnectionStringBase1 = "DataSource=@SERVERNAME@;Naming=@DB2ISERIESNAMING@;librarylist=@DB2ISERIESLIBRARYLIST@;UserID=@USERNAME@;Password=@PASSWORD@;";
        private string mDB2iSeriesNaming = "System";
        private string mDB2iSeriesLibraryList = "";
      
        //MySQL
        public string MySQLConnectionStringBase1 = "server=@SERVERNAME@;user id=@USERNAME@;password=@PASSWORD@;database=@DBNAME@;port=@MYSQLPORT@;";
        private string mMySQLPort = "3306";

        
        private bool mTransactionPending = false;
        public DbTransaction DbTransaction = null;
        public RuntimeUI RuntimeUI = RuntimeUI.WindowsForms;
        public object ValidatorLabelControl = null;
        public bool UseParallelism = false;


        private bool mDbConnectionKeepOpen = true;
        public bool DbConnectionKeepOpen
        {
            get
            {
                return mDbConnectionKeepOpen;
            }
            set
            {
                mDbConnectionKeepOpen = value;
            }
        }

        public string CloneOf
        {
            get
            {
                return mCloneOf; 
            }
        }

        public string Name
        {
            get
            {
                return mName;
            }
            set
            {
                mName = value;
            }
        }

        public string OptionalConnectionStringParameters
        {
            get
            {
                return mOptionalConnectionStringParameters;
            }
            set
            {
                mOptionalConnectionStringParameters = value;
            }
        }

        public ExecutionResult LoadACLFromXMLFile(string XMLFile)
        {
            ExecutionResult.Reset();
            try
            {
                var srReader = File.OpenText(XMLFile);
                var tType = mACL.GetType();
                var xsSerializer = new XmlSerializer(tType);
                var oData = xsSerializer.Deserialize(srReader);
                srReader.Close();
                mACL = (DbACL)oData;
            }
            catch (Exception ex)
            {
                ExecutionResult.Exception = ex;
                ExecutionResult.ErrorCode = 1;
                ExecutionResult.Context = "BasicDAL.DbConfig.LoadACLFromXMLFile";
                ExecutionResult.ResultMessage = ex.Message;
                ExecutionResult.ResultCode = (ExecutionResult.eResultCode)1;
            }

            return ExecutionResult;
        }

        public ExecutionResult LoadACLFromXMLString(string XMLString)
        {
            ExecutionResult.Reset();
            mACL = new DbACL();
            try
            {
                var srReader = new StringReader(XMLString);
                var tType = mACL.GetType();
                var xsSerializer = new XmlSerializer(tType);
                var oData = xsSerializer.Deserialize(srReader);
                srReader.Close();
                mACL = (DbACL)oData;
            }
            catch (Exception ex)
            {
                ExecutionResult.Exception = ex;
                ExecutionResult.ErrorCode = 1;
                ExecutionResult.Context = "BasicDAL.DbConfig.LoadACLFromXMLString";
                ExecutionResult.ResultMessage = ex.Message;
                ExecutionResult.ResultCode = (ExecutionResult.eResultCode)1;
            }

            return ExecutionResult;
        }

        public bool LoadFromConnectionString(string UserConnectionString, Providers Provider)
        {
            var builder = new DbConnectionStringBuilder();
            try
            {
                builder.ConnectionString = UserConnectionString;
                switch (Provider)
                {
                    case Providers.SqlServer:
                        {
                            ServerName = builder["server"] as string;
                            DataBaseName = builder["database"] as string;
                            UserName = builder["user id"] as string;
                            Password = builder["password"] as string;
                            switch (builder["trusted_connection"] as string ?? "")
                            {
                                case "yes":
                                    {
                                        AuthenticationMode = 1;
                                        break;
                                    }

                                case "no":
                                    {
                                        AuthenticationMode = 0;
                                        break;
                                    }
                            }

                            break;
                        }

                    default:
                        {
                            break;
                        }
                }

                Init();
                return true;
            }
            catch
            {
                return false;
            }
        }

        public Dictionary<string, DbObject> DbObjects = new Dictionary<string, DbObject>();

        public ExecutionResult DbObjectsInit()
        {
            var _ExecutionResult = new ExecutionResult();
            foreach (DbObject DbObject in DbObjects.Values)
            {
                DbObject.Init(this);
                if (DbObject.ExecutionResult.Failed)
                {
                    DbObject.Dispose();
                    break;
                }
            }

            return _ExecutionResult;
        }

        public DbProviderFactory DbProviderFactory
        {
            get
            {
                return objFactory;
            }
        }



        public DbACL ACL
        {
            get
            {
                return mACL;
            }

            set
            {
                mACL = value;
            }
        }

        public DateTime NullDateValue
        {
            get
            {
                return mNullDateValue;
            }

            set
            {
                mNullDateValue = value;
            }
        }

        public object RedirectErrorsNotificationTo
        {
            get
            {
                return mRedirectErrorsNotificationTo;
            }

            set
            {
                mRedirectErrorsNotificationTo = value;
            }
        }

        public bool SuppressErrorsNotification
        {
            get
            {
                return mSuppressErrorsNotification;
            }

            set
            {
                mSuppressErrorsNotification = value;
            }
        }

        public bool OnlyEntityInitialization
        {
            get
            {
                return mOnlyEntityInitialization;
            }

            set
            {
                mOnlyEntityInitialization = value;
            }
        }


        public string ServerVersion()
        {

            if (mDbConnection == null)
                this.Init();

            if (mDbConnection .State!= System.Data.ConnectionState.Open )
                mDbConnection.Open ();

            string s = mDbConnection.ServerVersion;

            if (mDbConnectionKeepOpen == false)
                mDbConnection.Close();

            return s;

        }

        public string GetOracleVersion()
        {
            
            return ServerVersion();
        }

        private string OldGetOracleVersion()
        {
            string OracleVersion = "";
            if (DbConnection.State == System.Data.ConnectionState.Open)
            {
                switch (Provider)
                {
                    case Providers.OracleDataAccess:
                        try
                        {
                            DbCommand objcommand = this.objFactory.CreateCommand();
                            objcommand.Connection = this.DbConnection;
                            objcommand.CommandType = CommandType.Text;
                            objcommand.CommandText = "SELECT version FROM v$instance";
                            OracleVersion = objcommand.ExecuteScalar().ToString();
                        }
                        catch (Exception ex)
                        {
                        }
                        break;

                    default:
                        {
                            break;
                        }
                }
            }

            return OracleVersion;
        }

        public bool Close()
        {
            try
            {
                if (DbConnection is null == false)
                {
                    if (DbConnection.State != System.Data.ConnectionState.Closed)
                    {
                        DbConnection.Close();
                        return true;
                    }
                    else
                    {
                        return true;
                    }
                }
                else
                {
                    return true;
                }
            }
            catch
            {
                return false;
            }
        }

        public DbConfig Clone(string Name=null)
        {

            if (Name == null)
                Name = "Clone of " + this.mName;

            var dbconfig = new DbConfig(Name);
            dbconfig.mCloneOf = this.mName;
            dbconfig.AuthenticationMode = AuthenticationMode;
            dbconfig.CommandTimeout = CommandTimeout;
            dbconfig.ConnectionTimeout = ConnectionTimeout;
            dbconfig.DataBaseName = DataBaseName;
            dbconfig.HandleErrors = HandleErrors;
            dbconfig.LogErrors = LogErrors;
            dbconfig.LogFile = LogFile;
            dbconfig.OraclePooling = OraclePooling;
            dbconfig.OraclePort = OraclePort;
            dbconfig.OracleServiceNameSID = OracleServiceNameSID;
            dbconfig.OracleUseSID = OracleUseSID;
            dbconfig.ParameterNamePrefix = ParameterNamePrefix;
            dbconfig.ParametersStyle = ParametersStyle;
            dbconfig.Password = Password;
            dbconfig.Provider = Provider;
            dbconfig.ServerName = ServerName;
            dbconfig.SupportSquareBrackets = SupportSquareBrackets;
            dbconfig.SupportTopRecords = SupportTopRecords;
            dbconfig.UserName = UserName;
            dbconfig.ConnectionString = ConnectionString;
            dbconfig.RedirectErrorsNotificationTo = RedirectErrorsNotificationTo;
            dbconfig.NullDateValue = NullDateValue;
            dbconfig.DbConnectionKeepOpen = DbConnectionKeepOpen;
            dbconfig.Init(true);
            
            return dbconfig;
        }

        public void BeginTransaction(IsolationLevel IsolationLevel = IsolationLevel.Unspecified)
        {
            try
            {
                if (this.mTransactionPending == false)
                {
                    this.DbTransaction = this.mDbConnection.BeginTransaction(IsolationLevel);
                    this.mTransactionPending = true;
                }
            }
            catch (Exception ex)
            {
                this.mTransactionPending = false;
                HandleExceptions(ex, "DBConfig.BeginTransaction");
            }
        }

        public bool CommitTransaction()
        {
            try
            {
                if (this.mTransactionPending)
                {
                    this.DbTransaction.Commit();
                    this.mTransactionPending = false;
                    this.DbTransaction = null;
                }

                return true;
            }
            catch (Exception ex)
            {
                this.DbTransaction.Rollback();
                HandleExceptions(ex, "DBConfig.CommitTransaction");
                this.mTransactionPending = false;
                this.DbTransaction = null;
                return false;
            }
        }

        public bool RollBackTransaction()
        {
            try
            {
                if (this.mTransactionPending)
                {
                    this.DbTransaction.Rollback();
                    this.mTransactionPending = false;
                    this.DbTransaction = null;
                }

                return true;
            }
            catch (Exception ex)
            {
                HandleExceptions(ex, "DBConfig.RollBackTransaction");
                this.mTransactionPending = false;
                this.DbTransaction = null;
                return false;
            }
        }

        public bool TransactionPending()
        {
            return this.mTransactionPending;
        }

        public bool HandleErrors
        {
            get
            {
                return boolHandleErrors;
            }

            set
            {
                boolHandleErrors = value;
            }
        }

        public bool ErrorState
        {
            get
            {
                return mErrorState;
            }

            set
            {
                mErrorState = value;
            }
        }

        public void ResetError()
        {
            ExecutionResult.Reset();
            mErrorState = false;
            strLastError = "";
            exLasteException = null;
            intLastErrorCode = 0;
        }

        public string LastError
        {
            get
            {
                return strLastError;
            }
        }

        public int LastErrorCode
        {
            get
            {
                return intLastErrorCode;
            }
        }

        public Exception LastErrorException
        {
            get
            {
                return exLasteException;
            }
        }

        public bool LogErrors
        {
            get
            {
                return boolLogError;
            }

            set
            {
                boolLogError = value;
            }
        }

        public string LogFile
        {
            get
            {
                return strLogFile;
            }

            set
            {
                strLogFile = value;
            }
        }

        public void Oracle_CaseSensitiveQuery(bool Mode)
        {
            // impostazione case sensitive ORACLE

            if (DbConnection.State == System.Data.ConnectionState.Open)
            {

                switch (Provider)
                {
                    case Providers.OracleDataAccess:

                        DbCommand objcommand = this.objFactory.CreateCommand();
                        objcommand.Connection = this.DbConnection;
                        objcommand.CommandType = System.Data.CommandType.Text;
                        if (Mode)
                        {
                            objcommand.CommandText = "ALTER SESSION SET NLS_COMP=LINGUISTIC";
                            objcommand.ExecuteNonQuery();
                            objcommand.CommandText = "ALTER SESSION SET NLS_SORT=BINARY_CI";
                            objcommand.ExecuteNonQuery();
                        }
                        else
                        {
                            objcommand.CommandText = "ALTER SESSION SET NLS_COMP=LINGUISTIC";
                            objcommand.ExecuteNonQuery();
                            objcommand.CommandText = "ALTER SESSION SET NLS_SORT=BINARY";
                            objcommand.ExecuteNonQuery();
                        }


                        break;
                    default:
                        {
                            break;
                        }
                }
            }
        }

        public void CaseSensitiveQuery(bool State)
        {
            // impostazione case sensitive ORACLE

            if (DbConnection.State == System.Data.ConnectionState.Open)
            {
                switch (Provider)
                {
                    case Providers.OracleDataAccess:

                        DbCommand objcommand = this.objFactory.CreateCommand();
                        objcommand.Connection = this.DbConnection;
                        objcommand.CommandType = System.Data.CommandType.Text;
                        if (State == true)
                        {
                            objcommand.CommandText = "ALTER SESSION SET NLS_COMP=LINGUISTIC";
                            objcommand.ExecuteNonQuery();
                            objcommand.CommandText = "ALTER SESSION SET NLS_SORT=BINARY_CI";
                            objcommand.ExecuteNonQuery();
                        }
                        else
                        {
                            objcommand.CommandText = "ALTER SESSION SET NLS_COMP=LINGUISTIC";
                            objcommand.ExecuteNonQuery();
                            objcommand.CommandText = "ALTER SESSION SET NLS_SORT=BINARY";
                            objcommand.ExecuteNonQuery();
                        }

                        break;
                    default:
                        {
                            break;
                        }
                }
            }
        }

        public void Oracle_NLS_LENGTH_SEMANTICS(int Mode)
        {
            // impostazione case sensitive ORACLE

            if (DbConnection.State == System.Data.ConnectionState.Open)
            {


                switch (Provider)
                {
                    case Providers.OracleDataAccess:

                        DbCommand objcommand = this.objFactory.CreateCommand();
                        objcommand.Connection = this.DbConnection;
                        objcommand.CommandType = System.Data.CommandType.Text;
                        switch (Mode)
                        {
                            case 1: // CHAR
                                {
                                    objcommand.CommandText = "ALTER session SET nls_length_semantics=CHAR";
                                    objcommand.ExecuteNonQuery();
                                    break;
                                }

                            default:
                                {
                                    objcommand.CommandText = "ALTER session SET nls_length_semantics=byte";
                                    objcommand.ExecuteNonQuery();
                                    break;
                                }
                        }

                        break;
                    default:
                        {
                            break;
                        }
                }
            }
        }


        private int InformationErr()
        {
            return Microsoft.VisualBasic.Information.Err().Number;
        }
        private void HandleExceptions(Exception ex, string Context = "")
        {
            string errmsg = "";
            string vbCrLf = "\r\n";

            errmsg = errmsg + "DbObject         : " + GetType().Name + vbCrLf;
            errmsg = errmsg + "Database Name    : " + mDatabaseName + vbCrLf;
            errmsg = errmsg + "Server Name      : " + mServerName + vbCrLf;
            errmsg = errmsg + "DbProvider       : " + mProvider.ToString() + vbCrLf;
            errmsg = errmsg + "Connectionstring : " + mConnectionString + vbCrLf;
            errmsg = errmsg + "Username         : " + mUserName + vbCrLf;
            errmsg = errmsg + vbCrLf;
            errmsg = errmsg + "Context          : " + Context + vbCrLf;
            errmsg = errmsg + "Error Source     : " + ex.Source + vbCrLf;
            errmsg = errmsg + "Error Code       : " + Information.Err().Number + vbCrLf;
            errmsg = errmsg + "Error Message    : " + vbCrLf;
            errmsg = errmsg + ex.Message + vbCrLf + vbCrLf;
            strLastError = Information.Err().Description;
            exLasteException = ex;
            intLastErrorCode = Information.Err().Number;
            ErrorState = true;
            ExecutionResult.Reset();
            ExecutionResult.ResultCode = ExecutionResult.eResultCode.Failed; //(ExecutionResult.eResultCode)Information.Err().Number;
            ExecutionResult.ResultMessage = ex.Message;
            ExecutionResult.ErrorCode = Information.Err().Number;
            ExecutionResult.Exception = ex;
            if (LogErrors)
            {
                WriteToLog(ex.Message);
            }

            if (HandleErrors)
            {
                if (mSuppressErrorsNotification == false)
                {
                    if (mRedirectErrorsNotificationTo != null)
                    {
                        Interaction.CallByName(mRedirectErrorsNotificationTo, "Show", CallType.Method, errmsg);
                        //Utilities .CallByName(mRedirectErrorsNotificationTo, "Show", Utilities.CallType.Method, errmsg);
                    }
                    else
                    {
                        Interaction.MsgBox(errmsg, MsgBoxStyle.Exclamation, "Error on Data Management");
                        //Utilities.CallByName(mRedirectErrorsNotificationTo, "Show", Utilities.CallType.Method, errmsg);
                    }
                }
            }
            else
            {
                throw ex;
            }
        }

        private void WriteToLog(string msg)
        {
            try
            {
                var writer = File.AppendText(LogFile);
                writer.WriteLine(DateTime.Now.ToString() + " - " + msg);
                writer.Close();
            }
            catch
            {
            }
        }

        public DbConnection DbConnection
        {
            get
            {
                DbConnection DBConnectionRet = default;
                DBConnectionRet = mDbConnection;
                return DBConnectionRet;
            }

            set
            {
                mDbConnection = value;
            }
        }

        public bool SupportTopRecords
        {
            get
            {
                bool SupportTopRecordsRet = default;
                SupportTopRecordsRet = mSupportTopRecords;
                return SupportTopRecordsRet;
            }

            set
            {
                mSupportTopRecords = value;
            }
        }

        public bool SupportSquareBrackets
        {
            get
            {
                bool SupportSquareBracketsRet = default;
                SupportSquareBracketsRet = mSupportSquareBrackets;
                return SupportSquareBracketsRet;
            }

            set
            {
                mSupportSquareBrackets = Convert.ToBoolean(value);
            }
        }

        public string OracleServiceNameSID
        {
            get
            {
                string OracleServiceNameSIDRet = default;
                OracleServiceNameSIDRet = mOracleServiceNameSID;
                return OracleServiceNameSIDRet;
            }

            set
            {
                mOracleServiceNameSID = value;
            }
        }

        public bool OracleUseSID
        {
            get
            {
                bool OracleUseSIDRet = default;
                OracleUseSIDRet = mOracleUseSID;
                return OracleUseSIDRet;
            }

            set
            {
                mOracleUseSID = value;
            }
        }

        public string OracleVersion
        {
            get
            {
                mOracleVersion = this.GetOracleVersion();
                return mOracleVersion;
            }
        }

        public bool OraclePooling
        {
            get
            {
                bool OraclePoolingRet = default;
                OraclePoolingRet = mOraclePooling;
                return OraclePoolingRet;
            }

            set
            {
                mOraclePooling = value;
            }
        }

        public int OracleMinPoolSize
        {
            get
            {
                int OracleMinPoolSizeRet = default;
                OracleMinPoolSizeRet = mOracleMinPoolSize;
                return OracleMinPoolSizeRet;
            }

            set
            {
                mOracleMinPoolSize = value;
            }
        }

        public int OracleMaxPoolSize
        {
            get
            {
                int OracleMaxPoolSizeRet = default;
                OracleMaxPoolSizeRet = mOracleMaxPoolSize;
                return OracleMaxPoolSizeRet;
            }

            set
            {
                mOracleMaxPoolSize = value;
            }
        }

        public int OraclePort
        {
            get
            {
                int OraclePortRet = default;
                OraclePortRet = mOraclePort;
                return OraclePortRet;
            }

            set
            {
                mOraclePort = value;
            }
        }

        public ParameterStyles ParametersStyle
        {
            get
            {
                ParameterStyles ParameterStyleRet = default;
                ParameterStyleRet = mParametersStyle;
                return ParameterStyleRet;
            }

            set
            {
                mParameterNamePrefix = ((int)value).ToString();
            }
        }

        public string ParameterNamePrefix
        {
            get
            {
                string ParameterNamePrefixRet = default;
                ParameterNamePrefixRet = mParameterNamePrefix;
                return ParameterNamePrefixRet;
            }

            set
            {
                mParameterNamePrefix = value;
            }
        }
        public string ParameterNamePrefixE
        {
            get
            {
                string ParameterNamePrefixRetE = default;
                ParameterNamePrefixRetE = mParameterNamePrefixE;
                return ParameterNamePrefixRetE;
            }

            set
            {
                mParameterNamePrefixE = value;
            }
        }
        public int CommandTimeout
        {
            get
            {
                int CommandTimeoutRet = default;
                CommandTimeoutRet = mCommandTimeout;
                return CommandTimeoutRet;
            }

            set
            {
                mCommandTimeout = value;
            }
        }

        public int PingConnectionTimeout
        {
            get
            {
                return mPingConnectionTimeout;
            }

            set
            {
                mPingConnectionTimeout = value;
            }
        }

        public int ConnectionTimeout
        {
            get
            {
                int ConnectionTimeoutRet = default;
                ConnectionTimeoutRet = mConnectionTimeout;
                return ConnectionTimeoutRet;
            }

            set
            {
                mConnectionTimeout = value;
            }
        }

        public Providers Provider
        {
            get
            {
                Providers ProviderRet = default;
                ProviderRet = mProvider;
                return ProviderRet;
            }

            set
            {
                mProvider = value;
            }
        }

        public string Password
        {
            get
            {
                string PasswordRet = default;
                PasswordRet = mPassword;
                return PasswordRet;
            }

            set
            {
                mPassword = value;
            }
        }

        public string UserName
        {
            get
            {
                string UserNameRet = default;
                UserNameRet = mUserName;
                return UserNameRet;
            }

            set
            {
                mUserName = value;
            }
        }

        public int AuthenticationMode
        {
            get
            {
                int AuthenticationModeRet = default;
                AuthenticationModeRet = mAuthenticationMode;
                return AuthenticationModeRet;
            }

            set
            {
                mAuthenticationMode = value;
            }
        }

        public string ConnectionString
        {
            get
            {
                string ConnectionStringRet = default;
                ConnectionStringRet = GetConnectionString();
                return ConnectionStringRet;
            }

            set
            {
                mConnectionString = value;
            }
        }

        public string ServerName
        {
            get
            {
                string ServerNameRet = default;
                ServerNameRet = mServerName;
                return ServerNameRet;
            }

            set
            {
                mServerName = value;
            }
        }

        public string DataBaseName
        {
            get
            {
                string DataBaseNameRet = default;
                DataBaseNameRet = mDatabaseName;
                return DataBaseNameRet;
            }

            set
            {
                mDatabaseName = value;
            }
        }


        public string MySQLPort
        {
            get
            {

                return mMySQLPort;
            }

            set
            {
                mMySQLPort  = value;
            }
        }

        public string DB2iSeriesLibraryList
        {
            get
            {

                return mDB2iSeriesLibraryList ;
            }

            set
            {
                mDB2iSeriesLibraryList = value;
            }
        }

        public string DB2iSeriesNaming
        {
            get
            {

                return mDB2iSeriesNaming ;
            }

            set
            {
                mDB2iSeriesNaming = value;
            }
        }
        public bool Init(string ConnectionString, bool ForceClose = false)
        {
            ResetError();
            try
            {
                if (DbConnection is object)
                {
                    if (ForceClose == false)
                    {
                        if (DbConnection.State != System.Data.ConnectionState.Closed)
                        {
                            DbConnection.Close();
                        }
                    }
                    else if (DbConnection.State != System.Data.ConnectionState.Closed)
                    {
                        return false;
                    }
                }

                switch (Provider)
                {
                    case Providers.SqlServer:
                        {
                            objFactory = SqlClientFactory.Instance;
                            break;
                        }

                    case Providers.OleDb:
                        {
                            objFactory = OleDbFactory.Instance;
                            break;
                        }

                    case Providers.OleDb_DB2:
                        {
                            objFactory = OleDbFactory.Instance;
                            break;
                        }

                    case Providers.DB2_iSeries:
                        {
                            objFactory = IBM.Data.DB2.iSeries.iDB2Factory.Instance;
                            break;
                        }

                    case Providers.MySQL :
                        {
                            objFactory = MySql.Data.MySqlClient.MySqlClientFactory.Instance;
                            break;
                        }

                    case Providers.OracleDataAccess:
#if COMPILEORACLE
                        this.objFactory = Oracle.ManagedDataAccess.Client.OracleClientFactory.Instance;
#endif
                        break;

                    case Providers.ODBC:
                        {
                            objFactory = OdbcFactory.Instance;
                            break;
                        }

                    case Providers.ODBC_DB2:
                        {
                            objFactory = OdbcFactory.Instance;
                            break;
                        }

                    case Providers.ConfigDefined:
                        {
                            string providername = ConfigurationManager.ConnectionStrings["connectionstring"].ProviderName;
                            switch (providername ?? "")
                            {
                                case "System.Data.SqlClient":
                                    {
                                        objFactory = SqlClientFactory.Instance;
                                        break;
                                    }

                                case "System.Data.OleDb":
                                    {
                                        objFactory = OleDbFactory.Instance;
                                        break;
                                    }

                                case "Oracle.DataAccess.Client":
                                    {
#if COMPILEORACLE
                                        objFactory = Oracle.ManagedDataAccess.Client.OracleClientFactory.Instance;
#endif
                                        break;
                                    }

                                case "System.Data.Odbc":
                                    {
                                        objFactory = OdbcFactory.Instance;
                                        break;
                                    }

                                default:
                                    {
                                        objFactory = SqlClientFactory.Instance;
                                        break;
                                    }
                            }

                            break;
                        }
                }

                if (DbConnection ==null)
                    DbConnection = objFactory.CreateConnection();

                DbConnection.ConnectionString = ConnectionString;
                    
                if (DbConnection .State !=System.Data.ConnectionState.Open )                
                    DbConnection.Open();




                switch (Provider)
                {
                    case Providers.SqlServer:
                        //var SQLConnection = (System.Data.SqlClient.SqlConnection)DbConnection;
                        //SQLConnection.WorkstationId 
                        break;
                    case Providers.OleDb:
                        break;
                    case Providers.OracleDataAccess:
                        break;
                    case Providers.ODBC:
                        break;
                    case Providers.ConfigDefined:
                        break;
                    case Providers.ODBC_DB2:
                        break;
                    case Providers.OleDb_DB2:
                        break;
                    case Providers.DB2_iSeries :
                        break;
                    case Providers.MySQL:
                        break;
                    default:
                        break;
                }





                var DBX = new DbConnectionStringBuilder();
                DBX.ConnectionString = ConnectionString;
                if (DbConnection.State == System.Data.ConnectionState.Open)
                {
                    DataBaseName = DbConnection.Database;
                    ServerName = DbConnection.DataSource;
                    this.ConnectionString = DbConnection.ConnectionString;
                    switch (Provider)
                    {
                        case Providers.SqlServer:
                            {
                                UserName = Convert.ToString(DBX["User Id"]);

                                if (DBX["trusted_connection"].ToString().ToLower() == "no")
                                {
                                    AuthenticationMode = 0;
                                }
                                else
                                {
                                    AuthenticationMode = 1;
                                }
                                CommandTimeout = Convert.ToInt32(DBX["connection timeout"]);
                                Password = Convert.ToString(DBX["password"]);
                                break;
                            }

                        case Providers.OleDb:
                            {
                                break;
                            }

                        case Providers.OleDb_DB2:
                            {
                                break;
                            }

                        case Providers.OracleDataAccess:
                            {
                                break;
                            }
                        case Providers.ODBC:
                            {
                                break;
                            }

                        case Providers.ODBC_DB2:
                            {
                                break;
                            }

                        case Providers.DB2_iSeries:
                            {
                                break;
                            }
                        case Providers.MySQL:
                            {
                                break;
                            }
                        default:
                            {
                                break;
                            }
                    }
                }
            }
            catch (Exception ex)
            {
                HandleExceptions(ex);
                if (mDbConnectionKeepOpen == false)
                    mDbConnection.Close();

                return false;
            }
            if (mDbConnectionKeepOpen == false)
                mDbConnection.Close();
            return true;
        }

        public bool Ping(int ConnectionTimeout = 10)
        {
            ResetError();
            DbConnection _DBConnection = null;
            DbProviderFactory _objFactory = null;
            try
            {
                switch (Provider)
                {
                    case Providers.SqlServer:
                        {
                            _objFactory = SqlClientFactory.Instance;
                            break;
                        }

                    case Providers.OleDb:
                        {
                            _objFactory = OleDbFactory.Instance;
                            break;
                        }

                    case Providers.OleDb_DB2:
                        {
                            _objFactory = OleDbFactory.Instance;
                            break;
                        }
                    case Providers.DB2_iSeries:
                        {
                            _objFactory = IBM.Data.DB2.iSeries.iDB2Factory.Instance;
                            break;
                        }

                    case Providers.MySQL :
                        {
                            _objFactory = MySql.Data.MySqlClient.MySqlClientFactory.Instance;
                            break;
                        }

                    case Providers.OracleDataAccess:

                        {
#if COMPILEORACLE
                            objFactory = Oracle.ManagedDataAccess.Client.OracleClientFactory.Instance;
#endif
                            break;
                        }

                    case Providers.ODBC:
                        {
                            _objFactory = OdbcFactory.Instance;
                            break;
                        }

                    case Providers.ODBC_DB2:
                        {
                            _objFactory = OdbcFactory.Instance;
                            break;
                        }

                    
                    case Providers.ConfigDefined:
                        {
                            string providername = ConfigurationManager.ConnectionStrings["connectionstring"].ProviderName;
                            switch (providername ?? "")
                            {
                                case "System.Data.SqlClient":
                                    {
                                        _objFactory = SqlClientFactory.Instance;
                                        break;
                                    }

                                case "System.Data.OleDb":
                                    {
                                        _objFactory = OleDbFactory.Instance;
                                        break;
                                    }

                                case "Oracle.DataAccess.Client":
                                    {
#if COMPILEORACLE
                                        _objFactory = Oracle.ManagedDataAccess.Client.OracleClientFactory.Instance;
#endif
                                        break;
                                    }


                                case "System.Data.Odbc":
                                    {
                                        _objFactory = OdbcFactory.Instance;
                                        break;
                                    }

                                default:
                                    {
                                        _objFactory = SqlClientFactory.Instance;
                                        break;
                                    }
                            }

                            break;
                        }
                }

                _DBConnection = _objFactory.CreateConnection();
                string cnstd = "Connection Timeout=15";
                string cnst = "Connection Timeout=" + ConnectionTimeout.ToString();
                string cns = GetConnectionString();
                if (cns.Contains(cnstd))
                {
                    cns = cns.Replace(cnstd, cnst);
                }

                _DBConnection.ConnectionString = cns;
                _DBConnection.Open();

                switch (Provider)
                {
                    case Providers.OracleDataAccess:
                        {
                            mOracleVersion = GetOracleVersion();
                            Oracle_CaseSensitiveQuery(false);
                            Oracle_NLS_LENGTH_SEMANTICS(1);
                            break;
                        }

                    default:
                        {
                            break;
                        }
                }
            }
            catch (Exception ex)
            {
                HandleExceptions(ex);
                _DBConnection.Close();
                _DBConnection.Dispose();
                return false;
            }
            _DBConnection.Close();
            _DBConnection.Dispose();
            return true;
        }

        public bool Init(bool ForceClose = false)
        {
            ResetError();
            try
            {
                if (DbConnection is object)
                {
                    if (ForceClose == false)
                    {
                        if (DbConnection.State != System.Data.ConnectionState.Closed)
                        {
                            DbConnection.Close();
                        }
                    }
                    else if (DbConnection.State != System.Data.ConnectionState.Closed)
                    {
                        return false;
                    }
                }

                switch (Provider)
                {
                    case Providers.SqlServer:
                        {
                            objFactory = SqlClientFactory.Instance;
                            break;
                        }

                    case Providers.OleDb:
                        {
                            objFactory = OleDbFactory.Instance;
                            break;
                        }

                    case Providers.OleDb_DB2:
                        {
                            objFactory = OleDbFactory.Instance;
                            break;
                        }

                    case Providers.OracleDataAccess:

                        {
#if COMPILEORACLE
                            objFactory = Oracle.ManagedDataAccess.Client.OracleClientFactory.Instance;
#endif
                            break;
                        }
                    case Providers.ODBC:
                        {
                            objFactory = OdbcFactory.Instance;
                            break;
                        }

                    case Providers.ODBC_DB2:
                        {
                            objFactory = OdbcFactory.Instance;
                            break;
                        }
                    case Providers.DB2_iSeries:
                        {
                            objFactory = IBM.Data.DB2.iSeries.iDB2Factory.Instance;
                            break;
                        }
                    case Providers.MySQL :
                        {
                            objFactory = MySql.Data.MySqlClient.MySqlClientFactory.Instance;
                            break;
                        }
                    case Providers.ConfigDefined:
                        {
                            string providername = ConfigurationManager.ConnectionStrings["connectionstring"].ProviderName;
                            switch (providername ?? "")
                            {
                                case "System.Data.SqlClient":
                                    {
                                        objFactory = SqlClientFactory.Instance;
                                        break;
                                    }

                                case "System.Data.OleDb":
                                    {
                                        objFactory = OleDbFactory.Instance;
                                        break;
                                    }
                                case "Oracle.DataAccess.Client":
                                    {
#if COMPILEORACLE
                                        objFactory = Oracle.ManagedDataAccess.Client.OracleClientFactory.Instance;
#endif
                                        break;
                                    }

                                case "System.Data.Odbc":
                                    {
                                        objFactory = OdbcFactory.Instance;
                                        break;
                                    }

                                default:
                                    {
                                        objFactory = SqlClientFactory.Instance;
                                        break;
                                    }
                            }

                            break;
                        }
                }

                DbConnection = objFactory.CreateConnection();
                DbConnection.ConnectionString = GetConnectionString();
                DbConnection.Open();
                switch (Provider)
                {
                    case Providers.OracleDataAccess:
                        {
                            mOracleVersion = GetOracleVersion();
                            Oracle_CaseSensitiveQuery(false);
                            Oracle_NLS_LENGTH_SEMANTICS(1);
                            break;
                        }
                    default:
                        {
                            break;
                        }
                }
            }
            catch (Exception ex)
            {
                HandleExceptions(ex);
                if (mDbConnectionKeepOpen == false)
                    mDbConnection.Close();
                return false;
            }

            if (mDbConnectionKeepOpen == false)
                mDbConnection.Close();
            return true;
        }

        public string GetConnectionString()
        {
            string x = "";
            switch (mProvider)
            {
                case Providers.SqlServer:
                    {
                        mParameterNamePrefix = "@";
                        mParameterNamePrefixE = "@p";
                        mSupportSquareBrackets = true;
                        mSupportTopRecords = true;
                        mParametersStyle = ParameterStyles.Qualified;

                        x = mConnectionString;
                        if (string.IsNullOrEmpty(x))
                        {
                            if (mAuthenticationMode == 1)
                            {
                                x = Convert.ToString(SQLServerConnectionStringBase2);
                            }
                            else
                            {
                                x = Convert.ToString(SQLServerConnectionStringBase2);
                            }

                            x = x.Replace("@DBNAME@", mDatabaseName);
                            x = x.Replace("@SERVERNAME@", mServerName);
                            x = x.Replace("@USERNAME@", mUserName);
                            x = x.Replace("@PASSWORD@", mPassword);
                            x = x.Replace("@CONNECTIONTIMEOUT@", mConnectionTimeout.ToString());
                        }

                        
                        break;
                    }

                case Providers.ODBC or Providers.ODBC_DB2:
                    {
                        mParameterNamePrefix = "?";
                        mParameterNamePrefixE = "?";


                        x = mConnectionString;
                        if (string.IsNullOrEmpty(x))
                        {
                            if (mAuthenticationMode == 1)
                            {
                                x = Convert.ToString(ODBCConnectionStringBase1);
                            }
                            else
                            {
                                x = Convert.ToString(ODBCConnectionStringBase2);
                            }
                            x = x.Replace("@USERNAME@", mUserName);
                            x = x.Replace("@PASSWORD@", mPassword);
                            x = x.Replace("@SERVERNAME@", mServerName);
                            x = x.Replace("@DBNAME@", mDatabaseName);
                        }

                        
                        break;
                    }

                case Providers.OleDb:
                    {
                        mParameterNamePrefix = "?";
                        mParameterNamePrefixE = "?";
                        mParametersStyle = ParameterStyles.Simple;
                        mSupportTopRecords = true;
                        x = mConnectionString;
                        if (string.IsNullOrEmpty(x))
                        {
                            if (mAuthenticationMode == 1)
                            {
                                x = Convert.ToString (OLEDBConnectionStringBase1);
                            }
                            else
                            {
                                x = Convert.ToString (OLEDBConnectionStringBase2);
                            }

                            x = x.Replace("@USERNAME@", mUserName);
                            x = x.Replace("@PASSWORD@", mPassword);
                            x = x.Replace("@SERVERNAME@", mServerName);
                            x = x.Replace("@DBNAME@", mDatabaseName);
                        }
                        break;
                    }

                case Providers.OleDb_DB2:
                    {
                        mParameterNamePrefix = "?";
                        mParameterNamePrefixE = "?";
                        mParametersStyle = ParameterStyles.Simple;
                        mSupportTopRecords = true;
                        x = mConnectionString;
                        if (string.IsNullOrEmpty(x))
                        {
                            x = Convert.ToString(OLEDB_DB2ConnectionStringBase1);
                            x = x.Replace("@OLEDB2PROVIDER@", mOleDB2Provider );
                            x = x.Replace("@OLEDB2NAMINGCONVENTION@", mOleDB2NamingConvention);
                            x = x.Replace("@OLEDB2PROVIDER@", mOleDB2Provider);
                            x = x.Replace("@OLEDB2INITIALCATALOG@", mOleDB2InitialCatalog );
                            x = x.Replace("@OLEDB2CONVERTDATETIMETOCHAR@", mOleDB2ConvertDateTimeToChar );
                            x = x.Replace("@USERNAME@", mUserName);
                            x = x.Replace("@PASSWORD@", mPassword);
                            x = x.Replace("@SERVERNAME@", mServerName);
                            x = x.Replace("@DBNAME@", mDatabaseName);
                        }
                        break;
                    }

                case Providers.DB2_iSeries :
                    {
                        mParameterNamePrefix = "@";
                        mParameterNamePrefixE = "@";
                        mParametersStyle = ParameterStyles.Qualified ;
                        mSupportTopRecords = true;

                        x = mConnectionString;
                        if (string.IsNullOrEmpty(x))
                        {
                            x = Convert.ToString(DB2_iSeriesConnectionStringBase1);

                            x = x.Replace("@USERNAME@", mUserName);
                            x = x.Replace("@PASSWORD@", mPassword);
                            x = x.Replace("@SERVERNAME@", mServerName);
                            x = x.Replace("@DBNAME@", mDatabaseName);
                            x = x.Replace("@DB2ISERIESLIBRARYLIST@", mDB2iSeriesLibraryList);
                            x = x.Replace("@DB2ISERIESNAMING@", mDB2iSeriesNaming );
                            
                        }
                        break;
                    }

                case Providers.MySQL:
                    {
                        mParameterNamePrefix = "@";
                        mParameterNamePrefixE = "@";
                        mParametersStyle = ParameterStyles.Qualified;
                        mSupportTopRecords = true;

                        x = mConnectionString;
                        if (string.IsNullOrEmpty(x))
                        {
                            x = Convert.ToString(MySQLConnectionStringBase1);

                            x = x.Replace("@USERNAME@", mUserName);
                            x = x.Replace("@PASSWORD@", mPassword);
                            x = x.Replace("@SERVERNAME@", mServerName);
                            x = x.Replace("@DBNAME@", mDatabaseName);
                            x = x.Replace("@MYSQLPORT@", mMySQLPort );
                        }

                        break;
                    }
                case Providers.OracleDataAccess:
                    {
                        mParameterNamePrefix = ":";
                        mParameterNamePrefixE = ":p";
                        mParametersStyle = ParameterStyles.Qualified;
                        mSupportTopRecords = true;
                        x = mConnectionString;
                        if (x == "")
                        {
                            if (this.mOracleUseSID == true)
                            {
                                x = this.OracleConnectionStringBase1;
                            }
                            else
                            {
                                x = this.OracleConnectionStringBase2;
                            }
                            
                        x = x.Replace("@USERNAME@", this.mUserName);
                        x = x.Replace( "@PASSWORD@", this.mPassword);
                        x = x.Replace( "@SERVERNAME@", this.mServerName);
                        x = x.Replace( "@SID@", this.mOracleServiceNameSID);
                        x = x.Replace( "@SERVICENAME@", this.mOracleServiceNameSID);
                        x = x.Replace( "@PORT@", this.mOraclePort.ToString());
                        x = x.Replace( "@ORACLEMAXPOOLSIZE@", this.mOracleMaxPoolSize.ToString());
                        x = x.Replace( "@ORACLEMINPOOLSIZE@", this.mOracleMinPoolSize.ToString());
                        x = x.Replace( "@ORACLEPOOLING@", this.mOraclePooling.ToString());
                        
                        }
                    

                        break;
                    }


                    //case Providers.Sharepoint:
                    //    {
                    //        ParameterNamePrefix = "";
                    //        x = mConnectionString;
                    //        x = x.Replace("@USERNAME@", mUserName);
                    //        x = x.Replace("@PASSWORD@", mPassword);
                    //        break;
                    //    }

                    //case Providers.ConfigDefined:
                    //    {
                    //        ParameterNamePrefix = "?";
                    //        x = mConnectionString;
                    //        x = x.Replace("@USERNAME@", mUserName);
                    //        x = x.Replace("@PASSWORD@", mPassword);
                    //        break;
                    //    }
            }

            if (x.EndsWith(";") == false)
                x = x + ";";
            
            x = x + mOptionalConnectionStringParameters;
            return x;

        }

        public DbConfig()
        {
            mName = System.Guid.NewGuid().ToString();
            ResetError();
        }

        public DbConfig(BasicDAL.Providers Provider)
        {
            mName = System.Guid.NewGuid().ToString();
            mProvider = Provider;
            ResetError();
        }
        public DbConfig(string Name)
        {
            if (Name==null)
                System.Guid.NewGuid().ToString();
            mName = Name;
            ResetError();
        }

        public DbConfig(string Name, BasicDAL .Providers Provider)
        {
            if (Name == null)
                System.Guid.NewGuid().ToString();
            mName = Name;
            mProvider = Provider;
            ResetError();
        }
    }

    #endregion

    #region BoundControl Class

    [Serializable()]
    public class BoundControl
    {
        private object _Control;
        private object _ValidationLabelControl;
        private string _PropertyName;
        private BindingBehaviour _BindingBehaviour;
        private int _RowsGroup = 0;
        private object _QueryValue;
        private System.Drawing.Color _BackColor;
        private System.Drawing.Color _ForeColor;
        private string _DateTimeCustomFormat = "";
        private bool _IsEdited = false;
        private object _OriginalValue = null;
        private string _Name = "";
        private bool _ReadOnly = true;

        public string Name
        {
            get
            {
                return _Name;
            }

            set
            {
                _Name = value;
            }
        }

        public object OriginalValue
        {
            get
            {
                return _OriginalValue;
            }

            set
            {
                _OriginalValue = value;
            }
        }

        public bool ReadOnly
        {
            get
            {
                return _ReadOnly;
            }

            set
            {
                _ReadOnly = value;
            }
        }

        public bool IsEdited
        {
            get
            {
                return _IsEdited;
            }

            set
            {
                _IsEdited = value;
            }
        }

        public string DateTimeCustomFormat
        {
            get
            {
                string DateTimeCustomFormatRet = default;
                DateTimeCustomFormatRet = _DateTimeCustomFormat;
                return DateTimeCustomFormatRet;
            }

            set
            {
                _DateTimeCustomFormat = value;
            }
        }

        public System.Drawing.Color BackColor
        {
            get
            {
                System.Drawing.Color BackColorRet = default;
                BackColorRet = _BackColor;
                return BackColorRet;
            }

            set
            {
                _BackColor = value;
            }
        }

        public System.Drawing.Color ForeColor
        {
            get
            {
                System.Drawing.Color ForeColorRet = default;
                ForeColorRet = _ForeColor;
                return ForeColorRet;
            }

            set
            {
                _ForeColor = value;
            }
        }

        public object QueryValue
        {
            get
            {
                object QueryValueRet = default;
                QueryValueRet = _QueryValue;
                return QueryValueRet;
            }

            set
            {
                _QueryValue = value;
            }
        }

        public int RowsGroup
        {
            get
            {
                int RowsGroupRet = default;
                RowsGroupRet = _RowsGroup;
                return RowsGroupRet;
            }

            set
            {
                _RowsGroup = value;
            }
        }

        public string PropertyName
        {
            get
            {
                string PropertyNameRet = default;
                PropertyNameRet = _PropertyName;
                return PropertyNameRet;
            }

            set
            {
                _PropertyName = value;
            }
        }

        public dynamic Control
        {
            get
            {
                dynamic ControlRet = default;
                ControlRet = _Control;
                return ControlRet;
            }

            set
            {
                _Control = value;
            }
        }

        public object ValidationLabelControl
        {
            get
            {
                object ValidationLabelControlRet = default;
                ValidationLabelControlRet = _ValidationLabelControl;
                return ValidationLabelControlRet;
            }

            set
            {
                _ValidationLabelControl = value;
            }
        }

        public BindingBehaviour BindingBehaviour
        {
            get
            {
                BindingBehaviour BindingBehaviourRet = default;
                BindingBehaviourRet = _BindingBehaviour;
                return BindingBehaviourRet;
            }

            set
            {
                _BindingBehaviour = value;
            }
        }
    }
    #endregion

    #region BoundControls Class
    [Serializable()]
    public class BoundControls : CollectionBase
    {
        public DbColumn DbColumn = null;

        public bool Add(object Control, string PropertyName = "text")
        {
            Add(Control, PropertyName, BindingBehaviour.ReadWrite);
            return default;
        }

        public bool Add(object Control, string PropertyName, BindingBehaviour BindingBehaviour)
        {
            try
            {
                var xControl = new BoundControl();
                xControl.Control = Control;
                xControl.PropertyName = PropertyName;
                xControl.BindingBehaviour = BindingBehaviour;
                try
                {
                    xControl.Name = Convert.ToString(Interaction.CallByName(Control, "Name", CallType.Get));
                }
                catch
                {
                }

                try
                {
                    xControl.BackColor = (System.Drawing.Color)Interaction.CallByName(Control, "BackColor", CallType.Get);
                }
                catch
                {
                }

                try
                {
                    xControl.DateTimeCustomFormat = Convert.ToString(Interaction.CallByName(Control, "CustomFormat", CallType.Get));
                }
                catch
                {
                }

                try
                {
                    xControl.ReadOnly = Convert.ToBoolean(Interaction.CallByName(Control, "ReadOnly", CallType.Get));
                }
                catch
                {
                }

                List.Add(xControl);
                return true;
            }
            catch
            {
                return false;
            }
        }

        public bool Add(int RowsGroup, object Control, string PropertyName, BindingBehaviour BindingBehaviour)
        {
            try
            {
                var xControl = new BoundControl();
                xControl.Control = Control;
                xControl.PropertyName = PropertyName;
                xControl.BindingBehaviour = BindingBehaviour;
                xControl.RowsGroup = RowsGroup;
                List.Add(xControl);
                return true;
            }
            catch
            {
                return false;
            }
        }
    }
    #endregion

    #region DbStoredProcedureParameter Class
    [Serializable()]
    public class DbParameter
    {
        public DbObject DbObject;
        private string mParameterName = string.Empty;
        private int mSize;
        private string mSourceColumn = string.Empty;
        private bool mSourceColumnNullMapping;
        private DataRowVersion mSourceVersion;
        private DbType mDbType = DbType.String;
        private ParameterDirection mDirection;
        private System.Data.Common.DbParameter mIDbParameter = null;
        private object mValue;

        public DbParameter()
        {

        }
        public DbParameter(string ParameterName, DbType DbType, ParameterDirection ParameterDirection, int Size = 0)
        {
            mParameterName = ParameterName;
            mDirection = ParameterDirection;
            mDbType = DbType;
            mSize = Size;
            if (DbObject is object)
            {
                mIDbParameter = DbObject.CreateDbParameter(ParameterName, DbType, ParameterDirection, Size);
            }
        }

        public object Value
        {
            get
            {
                if (mIDbParameter is object)
                {
                    mValue = mIDbParameter.Value;
                }

                return mValue;
            }

            set
            {
                mValue = value;
                if (mIDbParameter is object)
                {
                    mIDbParameter.Value = value;
                }
            }
        }

        // Public Property Value(ByVal IgnoreDbParameter As Boolean) As Object
        // Get
        // If IgnoreDbParameter = False Then
        // If mDbParameter IsNot Nothing Then
        // mValue = mDbParameter.Value
        // End If
        // End If

        // Return mValue

        // End Get
        // Set(ByVal value As Object)
        // mValue = value
        // If mDbParameter IsNot Nothing Then
        // mDbParameter.Value = value
        // End If
        // End Set
        // End Property
        public DataRowVersion SourceVersion
        {
            get
            {
                if (mIDbParameter is object)
                {
                    mSourceVersion = mIDbParameter.SourceVersion;
                }

                return mSourceVersion;
            }

            set
            {
                mSourceVersion = value;
                if (mIDbParameter is object)
                {
                    mIDbParameter.SourceVersion = value;
                }
            }
        }

        public bool SourceColumnNullMapping
        {
            get
            {
                if (mIDbParameter is object)
                {
                    mSourceColumnNullMapping = mIDbParameter.SourceColumnNullMapping;
                }

                return mSourceColumnNullMapping;
            }

            set
            {
                mSourceColumnNullMapping = value;
                if (mIDbParameter is object)
                {
                    mIDbParameter.SourceColumnNullMapping = value;
                }
            }
        }

        public string SourceColumn
        {
            get
            {
                if (mIDbParameter is object)
                {
                    mSourceColumn = mIDbParameter.SourceColumn;
                }

                return mSourceColumn;
            }

            set
            {
                mSourceColumn = value;
                if (mIDbParameter is object)
                {
                    mIDbParameter.SourceColumn = value;
                }
            }
        }

        public int Size
        {
            get
            {
                // If mDbParameter IsNot Nothing Then
                // mSize = mDbParameter.Size
                // End If
                return mSize;
            }

            set
            {
                mSize = value;
                if (mIDbParameter is object)
                {
                    mIDbParameter.Size = value;
                }
            }
        }

        public ParameterDirection Direction
        {
            get
            {
                if (mIDbParameter is object)
                {
                    mDirection = mIDbParameter.Direction;
                }

                return mDirection;
            }

            set
            {
                mDirection = value;
                if (mIDbParameter is object)
                {
                    mIDbParameter.Direction = value;
                }
            }
        }

        public DbType DbType
        {
            get
            {
                if (mIDbParameter is object)
                {
                    mDbType = mIDbParameter.DbType;
                }

                return mDbType;
            }

            set
            {
                mDbType = value;
                if (mIDbParameter is object)
                {
                    mIDbParameter.DbType = value;
                }
            }
        }

        public string ParameterName
        {
            get
            {
                if (mIDbParameter is object)
                {
                    mParameterName = mIDbParameter.ParameterName;
                }

                return mParameterName;
            }

            set
            {
                mParameterName = value;
                if (mIDbParameter is object)
                {
                    mIDbParameter.ParameterName = value;
                }
            }
        }

        public System.Data.Common.DbParameter IDbParameter
        {
            get
            {
                return mIDbParameter;
            }

            set
            {
                mIDbParameter = value;
            }
        }
    }

    [Serializable()]
    public class DbParameters : CollectionBase
    {
        public bool Add(string Name, DbType DbType, ParameterDirection Direction)
        {
            BasicDAL.DbParameter p = new BasicDAL.DbParameter(Name, DbType, Direction);

            p.ParameterName = Name;
            p.DbType = DbType;
            p.Direction = Direction;
            try
            {
                List.Add(p);
                return true;
            }
            catch
            {
                return false;
            }
        }

        public bool Add(DbParameter DbParameter)
        {
            try
            {
                List.Add(DbParameter);
                return true;
            }
            catch
            {
                return false;
            }
        }

        public DbParameter get_Item(int Index)
        {
            DbParameter ItemRet = default;
            ItemRet = (DbParameter)List[Index];
            return ItemRet;
        }

        public void set_Item(int Index, DbParameter value)
        {
            List[Index] = value;
        }

        public DbParameters()
        {
            List.Clear();
        }
    }
    #endregion

    #region DbColumns Class
    [Serializable()]
    //public class DbColumns : CollectionBase
    public class DbColumns : KeyedCollection <string,DbColumn >
    {

        // Fondamentale  se si usa KeyedCollection
        protected override string GetKeyForItem(DbColumn  item) => item.DbColumnNameE ;
       
       

        public  DbColumn Add(DbColumn DbColumn, object BoundControl, string PropertyName)
        {
            
            try
            {
                int Index;
                DbColumn.BoundControls.Add(BoundControl, PropertyName);
                //Index = List.Add(DbColumn);
                //return (DbColumn)List[Index];

                if (this.Contains(DbColumn) == false)
                    this.Add(DbColumn);
                
                return (DbColumn);

            }
            catch (Exception ex)
            {
                throw new Exception(ToString(), ex);

            }
        }

        public DbColumn xAdd(DbColumn DbColumn)
        {
            // If DbColumn Is Nothing Then
            // Return Nothing
            // End If
            try
            {
                int Index;
                //Index = List.Add(DbColumn);
                //return (DbColumn)List[Index];
                if (this.Contains(DbColumn.Name )==false)
                    this.Add(DbColumn);
                
                return (DbColumn);
            }
            catch (Exception ex)
            {
                throw new Exception(ToString(), ex);

            }
        }

        public DbColumn get_Item(int Index)
        {
            DbColumn ItemRet = default;
            //ItemRet = (DbColumn)List[Index];
            ItemRet = this[Index];
            return ItemRet;
            
        }

        public void set_Item(int Index, DbColumn value)
        {
            //List[Index] = value;
            this[Index] = value;
        }

        public DbColumns()
        {
      
            this.Clear();
            //List.Clear();
        }
    }
    #endregion



    [Serializable()]
    public class DbUser
    {
        public string Name = "";
        public object Description = "";
        public string SID = "";
        public SIDTypes SIDType = SIDTypes.User;
        public bool Enabled = true;
    }

    [Serializable]
    public class DbACE
    {
        public string ACL;
        public ACEApplyTo ACEApplyTo;
        public string User;
        public ACEAccessMask AccessMask = ACEAccessMask.FullControl;
    }

    [Serializable]
    public class DbACL : SerializableDictionary<string, DbACE>
    {
        public DbACE Add(string User, ACEAccessMask AccessMask)
        {
            // some custom logic here
            DbACE DbAce = null;
            if (ContainsKey(User) == false)
            {
                DbAce = new DbACE();
                DbAce.User = User;
                DbAce.AccessMask = AccessMask;
                Add(User, DbAce);
            }

            return DbAce;
        }
    }


    #region DbColumn Class

    // <AttributeUsage(AttributeTargets.Class Or
    // AttributeTargets.Method)>
    [Serializable()]
    public class DbColumn
    {
        public ExecutionResult ExecutionResult = new ExecutionResult();
        private string mTableName = "";
        private string mQualifiedFriendlyName = "";
        private string mFriendlyName = "";
        private object mValue = null;
        private object mOriginalValue = null;
        private DbType mDbType;
        private DataColumn mDataColumn;
        private string mDbColumnName = "";
        private string mDbColumnNameE = "";
        private object mDefaultValue = DBNull.Value;
        private bool mIsPrimaryKey = false;
        private ForeingKey mIsForeignKey = ForeingKey.False;
        private bool mIsForeignKeyValidated = false;
        private Required mIsRequired = Required.False;
        private BoundControls mBoundControls = new BoundControls();
        private string mDisplayFormat = "";
        private bool mUseInQBE = true;
        private bool mDisplayInListView = true;
        private bool mDisplayInQBEResult = true;
        private string mDiplayFormat = "";
        private string mDbColumnNameAlias = "";
        private DbLookUp mDbLookup = new DbLookUp();
        private int mSize;
        private bool mAllowNull;
        private int mQBEResulGridColumnNumber;
        private bool mDataChanged = false;
        private string mName = "";
        internal bool mLinkedColumn = false;
        private DbObject mDbObject;
        private int mColumnOrdinal;
        private int mNumericPrecision;
        private int mNumericScale;
        private bool mIsUnique;
        private bool mIsKey;
        private bool mIsRowID;
        private string mBaseServerName;
        private string mBaseCatalogName;
        private string mBaseColumnName;
        private string mBaseTableName;
        private string mBaseSchemaName;
        private Type mDataType;
        private bool mAllowDBNull;
        private int mProviderType;
        private bool mIsAliased;
        private bool mIsByteSemantic;
        private bool mIsExpression;
        private bool mIsIdentity;
        private bool mIsAutoIncrement;
        private object mAutoIncrementSeed;
        private object mAutoIncrementStep;
        private bool mIsRowVersion;
        private bool mIsHidden;
        private bool mIsLong;
        private bool mIsReadOnly;
        private Type mProviderSpecificDataType;
        private string mDataTypeName;
        private string mudtAssemblyQualifiedName;
        private int mNonVersionProviderType;
        private bool mValidated;
        private string mQBEValue;
        private DateTimeResolution mDateTimeResolution = DateTimeResolution.Second;
        private bool mExtraction = true;
        private ValidationTypes mValidationType = ValidationTypes.None;
        private ValidationDataType mValidationDataType = ValidationDataType.Default;
        private object mValidationExpression = null;
        private DataTypeFamily mDataTypeFamily = DataTypeFamily.String;
        private string mValidationMessage = "Il valore di {0} non è valido";


        // Private mACL As Dictionary(Of String, DbACE) = New Dictionary(Of String, DbACE)

        // Property ACL() As Dictionary(Of String, DbACE)
        // Get
        // Return mACL
        // End Get
        // Set(value As Dictionary(Of String, DbACE))
        // mACL = value
        // End Set
        // End Property

        // Public Sub AddACL(ByVal DbACE As DbACE)
        // If Me.mACL.ContainsKey(DbACE.User) = False Then
        // Me.mACL.Add(DbACE.User, DbACE)
        // Else
        // Me.mACL(DbACE.User).AccessMask = DbACE.AccessMask
        // End If
        // End Sub

        // Public Sub AddACL(ByVal User As String, ByVal AccessMask As BasicDAL.AccessMask)
        // If Me.mACL.ContainsKey(User) = False Then
        // Dim _DbACE As BasicDAL.DbACE = New BasicDAL.DbACE()
        // _DbACE.User = User
        // _DbACE.AccessMask = AccessMask
        // Me.mACL.Add(User, _DbACE)
        // Else
        // Me.mACL(User).AccessMask = AccessMask
        // End If
        // End Sub


        private DefaultValueEvaluationMode mDefaultValueEvaluationMode = DefaultValueEvaluationMode.OnEntityCreation;

        public DefaultValueEvaluationMode DefaultValueEvaluationMode
        {
            get
            {
                return mDefaultValueEvaluationMode;
            }

            set
            {
                mDefaultValueEvaluationMode = value;
            }
        }

        private object mEncryptionProvider;

        public object EncryptionProvider
        {
            get
            {
                return mEncryptionProvider;
            }

            set
            {
                mEncryptionProvider = value;
            }
        }

        private string mEncryptionKey;

        public string EncryptionKey
        {
            get
            {
                return mEncryptionKey;
            }

            set
            {
                mEncryptionKey = value;
            }
        }

        private bool mEncrypted = false;

        public bool Encrypted
        {
            get
            {
                return mEncrypted;
            }

            set
            {
                mEncrypted = value;
            }
        }

        private DbACL mACL = new DbACL();

        public DbACL ACL
        {
            get
            {
                return mACL;
            }

            set
            {
                mACL = value;
            }
        }

        public string ValidationMessage
        {
            get
            {
                return mValidationMessage;
            }

            set
            {
                mValidationMessage = value;
            }
        }

        public DataTypeFamily DataTypeFamily
        {
            get
            {
                return mDataTypeFamily;
            }
        }

        public ValidationDataType ValidationDataType
        {
            get
            {
                return mValidationDataType;
            }

            set
            {
                mValidationDataType = value;
            }
        }

        public ValidationTypes ValidationType
        {
            get
            {
                return mValidationType;
            }

            set
            {
                mValidationType = value;
            }
        }

        public DateTimeResolution DateTimeResolution
        {
            get
            {
                return mDateTimeResolution;
            }

            set
            {
                mDateTimeResolution = value;
            }
        }

        public bool Extraction
        {
            get
            {
                bool ExtractionRet = default;
                ExtractionRet = mExtraction;
                return ExtractionRet;
            }

            set
            {
                mExtraction = value;
            }
        }

        public bool Validated
        {
            get
            {
                bool ValidatedRet = default;
                ValidatedRet = mValidated;
                return ValidatedRet;
            }

            set
            {
                mValidated = value;
            }
        }

        public Required IsRequired
        {
            get
            {
                Required IsRequiredRet = default;
                if (mIsPrimaryKey == true)
                    return Required.True;
                if (mIsForeignKey == ForeingKey.True)
                    return Required.True;
                IsRequiredRet = mIsRequired;
                return IsRequiredRet;
            }

            set
            {
                mIsRequired = value;
            }
        }

        public bool IsForeignKeyValidated
        {
            get
            {
                bool IsForeignKeyValidatedRet = default;
                IsForeignKeyValidatedRet = mIsForeignKeyValidated;
                return IsForeignKeyValidatedRet;
            }

            set
            {
                mIsForeignKeyValidated = value;
            }
        }

        public ForeingKey IsForeignKey
        {
            get
            {
                ForeingKey IsForeignKeyRet = default;
                IsForeignKeyRet = mIsForeignKey;
                return IsForeignKeyRet;
            }

            set
            {
                mIsForeignKey = value;
            }
        }

        public string QBEValue
        {
            get
            {
                string QBEValueRet = default;
                QBEValueRet = mQBEValue;
                return QBEValueRet;
            }

            set
            {
                mQBEValue = value;
            }
        }

        public string DbColumnNameE
        {
            get
            {
                string DbColumnNameERet = default;
                DbColumnNameERet = mDbColumnNameE;
                return DbColumnNameERet;
            }

            set
            {
                mDbColumnNameE = value;
            }
        }

        public string BaseSchemaName
        {
            get
            {
                string BaseSchemaNameRet = default;
                BaseSchemaNameRet = mBaseSchemaName;
                return BaseSchemaNameRet;
            }

            set
            {
                mBaseSchemaName = value;
            }
        }

        public int NumericPrecision
        {
            get
            {
                int NumericPrecisionRet = default;
                NumericPrecisionRet = mNumericPrecision;
                return NumericPrecisionRet;
            }

            set
            {
                mNumericPrecision = value;
            }
        }

        public int NumericScale
        {
            get
            {
                int NumericScaleRet = default;
                NumericScaleRet = mNumericScale;
                return NumericScaleRet;
            }

            set
            {
                mNumericScale = value;
            }
        }

        public bool IsUnique
        {
            get
            {
                bool IsUniqueRet = default;
                IsUniqueRet = mIsUnique;
                return IsUniqueRet;
            }

            set
            {
                mIsUnique = value;
            }
        }

        public bool IsKey
        {
            get
            {
                bool IsKeyRet = default;
                IsKeyRet = mIsKey;
                return IsKeyRet;
            }

            set
            {
                mIsKey = value;
            }
        }

        public bool IsRowID
        {
            get
            {
                bool IsKeyRet = default;
                IsKeyRet = mIsRowID;
                return IsKeyRet;
            }

            set
            {
                mIsKey = value;
            }
        }

        public bool IsExpression
        {
            get
            {
                bool IsExpressionRet = default;
                IsExpressionRet = mIsExpression;
                return IsExpressionRet;
            }

            set
            {
                mIsExpression = value;
            }
        }

        public string BaseServerName
        {
            get
            {
                string BaseServerNameRet = default;
                BaseServerNameRet = mBaseServerName;
                return BaseServerNameRet;
            }

            set
            {
                mBaseServerName = value;
            }
        }

        public string BaseCatalogName
        {
            get
            {
                string BaseCatalogNameRet = default;
                BaseCatalogNameRet = mBaseCatalogName;
                return BaseCatalogNameRet;
            }

            set
            {
                mBaseCatalogName = value;
            }
        }

        public string BaseColumnName
        {
            get
            {
                string BaseColumnNameRet = default;
                BaseColumnNameRet = mBaseColumnName;
                return BaseColumnNameRet;
            }

            set
            {
                mBaseColumnName = value;
            }
        }

        public string BaseTableName
        {
            get
            {
                string BaseTableNameRet = default;
                BaseTableNameRet = mBaseTableName;
                return BaseTableNameRet;
            }

            set
            {
                mBaseTableName = value;
            }
        }

        public Type DataType
        {
            get
            {
                Type DataTypeRet = default;
                DataTypeRet = mDataType;
                return DataTypeRet;
            }

            set
            {
                mDataType = value;
            }
        }

        public bool AllowDBNull
        {
            get
            {
                bool AllowDBNullRet = default;
                AllowDBNullRet = mAllowDBNull;
                return AllowDBNullRet;
            }

            set
            {
                mAllowDBNull = value;
            }
        }

        public int ProviderType
        {
            get
            {
                int ProviderTypeRet = default;
                ProviderTypeRet = mProviderType;
                return ProviderTypeRet;
            }

            set
            {
                mProviderType = value;
            }
        }

        public bool IsAliased
        {
            get
            {
                bool IsAliasedRet = default;
                IsAliasedRet = mIsAliased;
                return IsAliasedRet;
            }

            set
            {
                mIsAliased = value;
            }
        }

        public bool IsByteSemantic
        {
            get
            {
                bool ret = default;
                ret = mIsByteSemantic;
                return ret;
            }

            set
            {
                mIsByteSemantic = value;
            }
        }

        public bool IsIdentity
        {
            get
            {
                bool IsIdentityRet = default;
                IsIdentityRet = mIsIdentity;
                return IsIdentityRet;
            }

            set
            {
                mIsIdentity = value;
            }
        }

        public bool IsAutoIncrement
        {
            get
            {
                bool IsAutoIncrementRet = default;
                IsAutoIncrementRet = mIsAutoIncrement;
                return IsAutoIncrementRet;
            }

            set
            {
                mIsAutoIncrement = value;
            }
        }

        public object AutoIncrementStep
        {
            get
            {
                object AutoIncrementStepRet = default;
                AutoIncrementStepRet = mAutoIncrementStep;
                return AutoIncrementStepRet;
            }

            set
            {
                mAutoIncrementStep = value;
            }
        }

        public object AutoIncrementSeed
        {
            get
            {
                object AutoIncrementSeedRet = default;
                AutoIncrementSeedRet = mAutoIncrementSeed;
                return AutoIncrementSeedRet;
            }

            set
            {
                mAutoIncrementSeed = value;
            }
        }

        public bool IsRowVersion
        {
            get
            {
                bool IsRowVersionRet = default;
                IsRowVersionRet = mIsRowVersion;
                return IsRowVersionRet;
            }

            set
            {
                mIsRowVersion = value;
            }
        }

        public bool IsHidden
        {
            get
            {
                bool IsHiddenRet = default;
                IsHiddenRet = mIsHidden;
                return IsHiddenRet;
            }

            set
            {
                mIsHidden = value;
            }
        }

        public bool IsLong
        {
            get
            {
                bool IsLongRet = default;
                IsLongRet = mIsLong;
                return IsLongRet;
            }

            set
            {
                mIsLong = value;
            }
        }

        public bool IsReadOnly
        {
            get
            {
                bool IsReadOnlyRet = default;
                IsReadOnlyRet = mIsReadOnly;
                return IsReadOnlyRet;
            }

            set
            {
                mIsReadOnly = value;
            }
        }

        public Type ProviderSpecificDataType
        {
            get
            {
                Type ProviderSpecificDataTypeRet = default;
                ProviderSpecificDataTypeRet = mProviderSpecificDataType;
                return ProviderSpecificDataTypeRet;
            }

            set
            {
                mProviderSpecificDataType = value;
            }
        }

        public string DataTypeName
        {
            get
            {
                string DataTypeNameRet = default;
                DataTypeNameRet = mDataTypeName;
                return DataTypeNameRet;
            }

            set
            {
                mDataTypeName = value;
            }
        }

        public string udtAssemblyQualifiedName
        {
            get
            {
                string udtAssemblyQualifiedNameRet = default;
                udtAssemblyQualifiedNameRet = mudtAssemblyQualifiedName;
                return udtAssemblyQualifiedNameRet;
            }

            set
            {
                mudtAssemblyQualifiedName = value;
            }
        }

        public int NonVersionProviderType
        {
            get
            {
                int NonVersionProviderTypeRet = default;
                NonVersionProviderTypeRet = mNonVersionProviderType;
                return NonVersionProviderTypeRet;
            }

            set
            {
                mNonVersionProviderType = value;
            }
        }

        public int ColumnOrdinal
        {
            get
            {
                int ColumnOrdinalRet = default;
                ColumnOrdinalRet = mColumnOrdinal;
                return ColumnOrdinalRet;
            }

            set
            {
                mColumnOrdinal = value;
            }
        }

        public DbObject DbObject
        {
            get
            {
                DbObject DbObjectRet = default;
                DbObjectRet = mDbObject;
                return DbObjectRet;
            }

            set
            {
                mDbObject = value;
            }
        }

        public string Name
        {
            get
            {
                string NameRet = default;
                NameRet = mName;
                return NameRet;
            }

            set
            {
                mName = value;
            }
        }

        public bool DataChanged
        {
            get
            {
                bool DataChangedRet = default;
                DataChangedRet = mDataChanged;
                return DataChangedRet;
            }

            set
            {
                mDataChanged = value;
            }
        }

        public int QBEResulGridColumnNumber
        {
            get
            {
                int QBEResulGridColumnNumberRet = default;
                QBEResulGridColumnNumberRet = mQBEResulGridColumnNumber;
                return QBEResulGridColumnNumberRet;
            }

            set
            {
                mQBEResulGridColumnNumber = value;
            }
        }

        public bool LinkedColumn
        {
            get
            {
                bool LinkedColumnRet = default;
                LinkedColumnRet = mLinkedColumn;
                return LinkedColumnRet;
            }
        }

        public bool AllowNull
        {
            get
            {
                bool AllowNullRet = default;
                AllowNullRet = mAllowNull;
                return AllowNullRet;
            }

            set
            {
                mAllowNull = value;
            }
        }

        public int Size
        {
            get
            {
                int SizeRet = default;
                SizeRet = mSize;
                return SizeRet;
            }

            set
            {
                mSize = value;
            }
        }

        public DbLookUp DbLookup
        {
            get
            {
                DbLookUp DbLookupRet = default;
                DbLookupRet = mDbLookup;
                return DbLookupRet;
            }

            set
            {
                mDbLookup = value;
            }
        }

        public string DbColumnNameAlias
        {
            get
            {
                string DbColumnNameAliasRet = default;
                DbColumnNameAliasRet = mDbColumnNameAlias;
                return DbColumnNameAliasRet;
            }

            set
            {
                mDbColumnNameAlias = value;
            }
        }

        public DataColumn DataColumn
        {
            get
            {
                DataColumn DataColumnRet = default;
                DataColumnRet = mDataColumn;
                return DataColumnRet;
            }

            set
            {
                mDataColumn = value;
            }
        }

        public bool DisplayInQBEResult
        {
            get
            {
                bool DisplayInQBEResultRet = default;
                DisplayInQBEResultRet = mDisplayInQBEResult;
                return DisplayInQBEResultRet;
            }

            set
            {
                mDisplayInQBEResult = value;
            }
        }

        public bool UseInQBE
        {
            get
            {
                bool UseInQBERet = default;
                UseInQBERet = mUseInQBE;
                return UseInQBERet;
            }

            set
            {
                mUseInQBE = value;
            }
        }

        public bool DisplayInListView
        {
            get
            {
                bool DisplayInListViewRet = default;
                DisplayInListViewRet = mDisplayInListView;
                return DisplayInListViewRet;
            }

            set
            {
                mDisplayInListView = value;
            }
        }

        public string TableName
        {
            get
            {
                string TableNameRet = default;
                if (mTableName == "")
                {
                    int p;
                    p = mDbColumnName.IndexOf(".");
                    if (p > 0)
                        mTableName = DbColumnName.Substring(0, p);
                }

                TableNameRet = mTableName;
                return TableNameRet;
            }

            set
            {
                mTableName = value;
            }
        }

        public string DisplayFormat
        {
            get
            {
                string DisplayFormatRet = default;
                DisplayFormatRet = mDisplayFormat;
                return DisplayFormatRet;
            }

            set
            {
                mDisplayFormat = value;
            }
        }

        public BoundControls BoundControls
        {
            get
            {
                BoundControls BoundControlsRet = default;
                BoundControlsRet = mBoundControls;
                return BoundControlsRet;
            }

            set
            {
                mBoundControls = value;
            }
        }

        public bool IsPrimaryKey
        {
            get
            {
                bool IsPrimaryKeyRet = default;
                IsPrimaryKeyRet = mIsPrimaryKey;
                return IsPrimaryKeyRet;
            }

            set
            {
                mIsPrimaryKey = value;
            }
        }

        public object DefaultValue
        {
            get
            {
                object DefaultValueRet = default;
                DefaultValueRet = mDefaultValue;
                return DefaultValueRet;
            }

            set
            {
                mDefaultValue = value;
            }
        }

        public string DbColumnName
        {
            get
            {
                string DbColumnNameRet = default;
                DbColumnNameRet = mDbColumnName;
                return DbColumnNameRet;
            }

            set
            {
                mDbColumnName = value;
            }
        }

        public DbType DbType
        {
            get
            {
                DbType DbTypeRet = default;
                DbTypeRet = mDbType;
                return DbTypeRet;
            }

            set
            {
                mDbType = value;
                if (IsString())
                {
                    mDataTypeFamily = DataTypeFamily.String;
                }

                if (IsDate())
                {
                    mDataTypeFamily = DataTypeFamily.DateTime;
                }

                if (IsTime())
                {
                    mDataTypeFamily = DataTypeFamily.Time;
                }

                if (IsNumeric())
                {
                    mDataTypeFamily = DataTypeFamily.Numeric;
                }

                if (IsXML())
                {
                    mDataTypeFamily = DataTypeFamily.XML;
                }

                if (IsBoolean())
                {
                    mDataTypeFamily = DataTypeFamily.Boolean;
                }

                if (DbType == DbType.Binary)
                {
                    mDataTypeFamily = DataTypeFamily.Binary;
                }
            }
        }

        public object OriginalValue
        {
            get
            {
                object OriginalValueRet = default;
                OriginalValueRet = mOriginalValue;
                return OriginalValueRet;
            }

            set
            {
                mOriginalValue = value;
            }
        }

        public string FriendlyName
        {
            get
            {
                if (string.IsNullOrEmpty(mFriendlyName.Trim()))
                {
                    return DbColumnNameE;
                }
                else
                {
                    return mFriendlyName;
                }
            }

            set
            {
                mFriendlyName = value;
            }
        }

        public string FullQualifiedFriendlyName
        {
            get
            {
                return DbObject.FriendlyName + "." + FriendlyName;
            }
        }

        public object ValidationExpression
        {
            get
            {
                return mValidationExpression;
            }

            set
            {
                mValidationExpression = value;
            }
        }

        public bool IsDBNull()
        {
            if (mValue.Equals(DBNull.Value) == false)
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        public object Value
        {
            get
            {
                object ValueRet = default;
                ValueRet = mValue;
                return ValueRet;
            }

            set
            {
                Utilities.DBNullHandler(ref value);
                if (!ReferenceEquals(value, DBNull.Value))
                {
                    switch (DbType)
                    {
                        case DbType.AnsiString or DbType.AnsiStringFixedLength or DbType.String or DbType.StringFixedLength:
                            {
                                if (value.ToString().Length >= Size)
                                {
                                    if (Size > 0)
                                    {
                                        //mValue = Strings.Mid(Conversions.ToString(value), 1, Size);
                                        mValue = value.ToString().Substring(0, Size);
                                    }
                                    else
                                    {
                                        mValue = value;
                                    }
                                }
                                else
                                {
                                    mValue = value;
                                }

                                break;
                            }

                        default:
                            {
                                mValue = value;
                                break;
                            }
                    }
                }
                else
                {
                    mValue = value;
                }

                if (mDbObject.CurrentPosition >= 0)
                {
                    mDbObject.CurrentDataRow[mDbColumnNameE] = mValue;
                    // Me.mDbobject.CurrentDataRow(Me.mColumnOrdinal) = mValue
                    // Me.mDbobject.CurrentDataRow(Me.mDbColumnName) = mValue
                }

                mDataChanged = true;
                mValidated = false;
            }
        }

        public object get_Value(bool UpdateCurrentDataRow)
        {
            return mValue;
        }

        public void set_Value(bool UpdateCurrentDataRow, object value)
        {
            Utilities.DBNullHandler(ref value);
            if (!ReferenceEquals(value, DBNull.Value))
            {
                switch (DbType)
                {
                    case DbType.AnsiString or DbType.AnsiStringFixedLength or DbType.String or DbType.StringFixedLength:
                        {
                            //if (Strings.Len(value) >= Size)
                            if (value.ToString().Length >= Size)
                            {
                                if (Size > 0)
                                {
                                    //mValue = Strings.Mid(Conversions.ToString(value), 1, Size);
                                    mValue = value.ToString().Substring(0, Size - 1);
                                }
                                else
                                {
                                    mValue = value;
                                }
                            }
                            else
                            {
                                mValue = value;
                            }

                            break;
                        }

                    default:
                        {
                            mValue = value;
                            break;
                        }
                }

                if (UpdateCurrentDataRow == true)
                {
                    mDbObject.CurrentDataRow[mDbColumnNameE] = mValue;
                }
            }
            else
            {
                mValue = value;
            }

            if (UpdateCurrentDataRow == true)
            {
                mDbObject.CurrentDataRow[mDbColumnNameE] = mValue;
            }

            mDataChanged = true;
            mValidated = false;
        }

        public object ValueNoUpdateCurrentDataRow
        {
            get
            {
                return mValue;
            }

            set
            {
                Utilities.DBNullHandler(ref value);
                if (!ReferenceEquals(value, DBNull.Value))
                {
                    switch (DbType)
                    {
                        case DbType.AnsiString or DbType.AnsiStringFixedLength or DbType.String or DbType.StringFixedLength:
                            {

                                if (value.ToString().Length >= Size)
                                {
                                    if (Size > 0)
                                    {
                                        mValue = value.ToString().Substring(0, Size);
                                    }
                                    else
                                    {
                                        mValue = value;
                                    }
                                }
                                else
                                {
                                    mValue = value;
                                }

                                break;
                            }

                        default:
                            {
                                mValue = value;
                                break;
                            }
                    }
                }
                else
                {
                    mValue = value;
                }

                mDataChanged = true;
                mValidated = false;
            }
        }

        public bool Validate()
        {
            bool Validation = true;
            if (ValidationType == ValidationTypes.None)
            {
                return true;
            }

            string errmsg = "";
            switch (ValidationType)
            {
                case ValidationTypes.Range:
                    {
                        if (!(mValidationExpression is ValidationRange))
                        {
                            return Validation;
                        }

                        ValidationRange ValidationRange = (ValidationRange)mValidationExpression;
                        Validation = Validators.Range(mValue, ValidationRange.MinValue, ValidationRange.MaxValue);
                        if (Validation == false)
                        {
                            errmsg = string.Format(mValidationMessage, FriendlyName, ValidationRange.MinValue, ValidationRange.MaxValue);
                        }

                        break;
                    }

                case ValidationTypes.RegularExpression:
                    {
                        Validation = Validators.RegExp(mFriendlyName, Convert.ToString(mValidationExpression));
                        if (Validation == false)
                        {
                            errmsg = string.Format(mValidationMessage, FriendlyName, mValue);
                        }

                        break;
                    }

                case ValidationTypes.Equal:
                    {
                        Validation = Validators.Equal(mValue, mValidationExpression);
                        if (Validation == false)
                        {
                            errmsg = string.Format(mValidationMessage, FriendlyName);
                        }

                        break;
                    }

                case ValidationTypes.CompareList:
                    {
                        Validation = Validators.CompareList(mValue, mValidationExpression);
                        if (Validation == false)
                        {
                            errmsg = string.Format(mValidationMessage, FriendlyName, Validators.CompareListToString(mValidationExpression));
                        }

                        break;
                    }

                case ValidationTypes.Required:
                    {
                        Validation = Validators.Required(mValue);
                        if (Validation == false)
                        {
                            errmsg = string.Format(mValidationMessage, FriendlyName);
                        }

                        break;
                    }

                case ValidationTypes.ValidURI:
                    {
                        Validation = Validators.ValidURI(Convert.ToString(mValue));
                        if (Validation == false)
                        {
                            errmsg = string.Format(mValidationMessage, FriendlyName);
                        }

                        break;
                    }

                case ValidationTypes.ValidURL:
                    {
                        Validation = Validators.ValidURL(Convert.ToString(mValue));
                        if (Validation == false)
                        {
                            errmsg = string.Format(mValidationMessage, FriendlyName);
                        }

                        break;
                    }

                default:
                    {
                        break;
                    }
            }

            if (Validation == true)
            {
                if (mDbObject.SuppressErrorsNotification == false)
                {
                    if (DbObject.RedirectErrorsNotificationTo != null)
                    {
                        Interaction.CallByName(DbObject.RedirectErrorsNotificationTo, "Show", CallType.Method, mValidationMessage);

                    }
                    //else
                    //{
                    //    //Interaction.MsgBox(errmsg, MsgBoxStyle.Exclamation, "Errore nella Gestione dei Dati");
                    //    Utilities.CallByName(DbObject.RedirectErrorsNotificationTo, "Show", Utilities.CallType.Method, mValidationMessage);
                    //}
                }
            }

            return Validation;
        }

        public void SetValidationRule(ValidationTypes ValidationType, object ValidationExpression, string ValidationMessage = "")
        {
            mValidationType = ValidationType;
            mValidationExpression = ValidationExpression;
            mValidationMessage = ValidationMessage;
            SetValidationMessage();
        }

        public void SetValidationRule(ValidationTypes ValidationType, string ValidationMessage = "")
        {
            mValidationType = ValidationType;
            mValidationExpression = "";
            mValidationMessage = ValidationMessage;
            SetValidationMessage();
        }

        public void SetValidationRule(ValidationTypes ValidationType, object MinValue, object MaxValue, string ValidationMessage = "")
        {
            mValidationType = ValidationType;
            var ValidationRange = new ValidationRange();
            ValidationRange.MinValue = MinValue;
            ValidationRange.MaxValue = MaxValue;
            mValidationExpression = ValidationRange;
            mValidationMessage = ValidationMessage;
            SetValidationMessage();
        }

        private void SetValidationMessage()
        {
            if (string.IsNullOrEmpty(mValidationMessage))
            {
                switch (mValidationType)
                {
                    case ValidationTypes.CompareList:
                        {
                            mValidationMessage = "Il valore di [{0}] deve essere compreso fra questo elenco di valori {1}";
                            break;
                        }

                    case ValidationTypes.Equal:
                        {
                            mValidationMessage = "Il valore di [{0}] deve essere uguale a {1}.";
                            break;
                        }

                    case ValidationTypes.None:
                        {
                            mValidationMessage = "";
                            break;
                        }

                    case ValidationTypes.Range:
                        {
                            mValidationMessage = "Il valore di {0} deve essere compreso fra {1} e {2}.";
                            break;
                        }

                    case ValidationTypes.RegularExpression:
                        {
                            mValidationMessage = "Il valore di [{0}] non rispetta le regole previste.";
                            break;
                        }

                    case ValidationTypes.Required:
                        {
                            mValidationMessage = "Il valore di {0} non può essere nullo.";
                            break;
                        }

                    case ValidationTypes.ValidURI:
                        {
                            mValidationMessage = "Il valore di {0} non è una URI valida.";
                            break;
                        }

                    case ValidationTypes.ValidURL:
                        {
                            mValidationMessage = "Il valore di {0} non è una URL valida.";
                            break;
                        }

                    case ValidationTypes.ValidEmail:
                        {
                            mValidationMessage = "Il valore di {0} non è un indirizzo email valido.";
                            break;
                        }

                    default:
                        {
                            break;
                        }
                }
            }
        }

        public bool ValidateBoundControls()
        {
            bool Validation = true;
            if (ValidationType == ValidationTypes.None)
            {
                return true;
            }

            string errmsg = "";
            //foreach (BoundControl _BoundControl in mBoundControls)
            foreach (BoundControl _BoundControl in mBoundControls)
            {

                if (_BoundControl.BindingBehaviour != BindingBehaviour.Write & _BoundControl.ValidationLabelControl is object)
                {
                    bool ControlAllowNull = mAllowDBNull;
                    object ControlValue = null;

                    ControlValue = Interaction.CallByName(_BoundControl.Control, _BoundControl.PropertyName, CallType.Get);

                    switch (ValidationType)
                    {
                        case ValidationTypes.Range:
                            {
                                if (!(mValidationExpression is ValidationRange))
                                {
                                    return Validation;
                                }

                                ValidationRange ValidationRange = (ValidationRange)mValidationExpression;
                                Validation = Validators.Range(ControlValue, ValidationRange.MinValue, ValidationRange.MaxValue, ControlAllowNull);
                                if (Validation == false)
                                {
                                    errmsg = string.Format(mValidationMessage, FriendlyName, ValidationRange.MinValue, ValidationRange.MaxValue);
                                }

                                break;
                            }

                        case ValidationTypes.RegularExpression:
                            {
                                Validation = Validators.RegExp(ControlValue, Convert.ToString(mValidationExpression), ControlAllowNull);
                                if (Validation == false)
                                {
                                    errmsg = string.Format(mValidationMessage, FriendlyName, ControlValue);
                                }

                                break;
                            }

                        case ValidationTypes.Equal:
                            {
                                Validation = Validators.Equal(ControlValue, mValidationExpression, ControlAllowNull);
                                if (Validation == false)
                                {
                                    errmsg = string.Format(mValidationMessage, FriendlyName, mValidationExpression);
                                }

                                break;
                            }

                        case ValidationTypes.CompareList:
                            {
                                Validation = Validators.CompareList(ControlValue, mValidationExpression, ControlAllowNull);
                                if (Validation == false)
                                {
                                    errmsg = string.Format(mValidationMessage, FriendlyName, Validators.CompareListToString(mValidationExpression));
                                }

                                break;
                            }

                        case ValidationTypes.Required:
                            {
                                Validation = Validators.Required(ControlValue);
                                if (Validation == false)
                                {
                                    errmsg = string.Format(mValidationMessage, FriendlyName);
                                }

                                break;
                            }

                        case ValidationTypes.ValidURI:
                            {
                                Validation = Validators.ValidURI(Convert.ToString(ControlValue), ControlAllowNull);
                                if (Validation == false)
                                {
                                    errmsg = string.Format(mValidationMessage, FriendlyName);
                                }

                                break;
                            }

                        case ValidationTypes.ValidURL:
                            {
                                Validation = Validators.ValidURL(Convert.ToString(ControlValue), ControlAllowNull);
                                if (Validation == false)
                                {
                                    errmsg = string.Format(mValidationMessage, FriendlyName);
                                }

                                break;
                            }

                        case ValidationTypes.ValidEmail:
                            {
                                Validation = Validators.ValidEmail(Convert.ToString(ControlValue), ControlAllowNull);
                                if (Validation == false)
                                {
                                    errmsg = string.Format(mValidationMessage, FriendlyName);
                                }

                                break;
                            }

                        default:
                            {
                                break;
                            }
                    }

                    switch (mDbObject.RuntimeUI)
                    {
                        case RuntimeUI.Service:
                            {
                                break;
                            }
                        //case RuntimeUI.Wisej:
                        case RuntimeUI.WindowsForms or RuntimeUI.Wisej:
                            {
                                if (Validation == true)
                                {

                                    Interaction.CallByName(_BoundControl, "Tag", CallType.Set, null);
                                    Interaction.CallByName(_BoundControl, "Visible", CallType.Set, false);
                                    //_BoundControl.ValidationLabelControl.Tag = "";
                                    //_BoundControl.ValidationLabelControl.Visible = false;
                                }
                                else
                                {
                                    Interaction.CallByName(_BoundControl, "Tag", CallType.Set, errmsg);
                                    Interaction.CallByName(_BoundControl, "Visible", CallType.Set, true);
                                    Interaction.CallByName(_BoundControl, "BringToFront", CallType.Method, null);
                                    //_BoundControl.ValidationLabelControl.Tag = errmsg;
                                    //_BoundControl.ValidationLabelControl.Visible = true;
                                    //_BoundControl.ValidationLabelControl.BringToFront();
                                }

                                break;
                            }

                        case RuntimeUI.Web:
                            {
                                break;
                            }
                    }
                }
            }

            return Validation;
        }

        public bool IsSortable()
        {
            bool _sortable = true;
            switch (mDbType)
            {
                case DbType.Binary or DbType.Object:
                    {
                        _sortable = false;
                        break;
                    }

                default:
                    {
                        switch (DbObject.DbConfig.Provider)
                        {
                            case Providers.SqlServer:
                                {
                                    switch (DataTypeName.ToLower() ?? "")
                                    {
                                        case "text":
                                        case "ntext":
                                            {
                                                _sortable = false;
                                                break;
                                            }
                                    }

                                    break;
                                }

                            default:
                                {
                                    break;
                                }
                        }

                        break;
                    }
            }

            return _sortable;
        }

        public bool IsSearchable()
        {
            switch (mDbType)
            {
                case DbType.Binary or DbType.Object:
                    {
                        return false;
                    }

                default:
                    {
                        return false;
                    }
            }
        }

        public bool IsBinary()
        {
            switch (mDbType)
            {
                case DbType.Binary or DbType.Object:
                    {
                        return true;
                    }

                default:
                    {
                        return false;
                    }
            }
        }

        public bool IsDate()
        {
            switch (mDbType)
            {
                case DbType.Date or DbType.DateTime or DbType.DateTime2:
                    {
                        return true;
                    }

                default:
                    {
                        return false;
                    }
            }
        }

        public bool IsTime()
        {
            switch (mDbType)
            {
                case DbType.Time:
                    {
                        return true;
                    }

                default:
                    {
                        return false;
                    }
            }
        }

        public bool IsXML()
        {
            switch (mDbType)
            {
                case DbType.Xml:
                    {
                        return true;
                    }

                default:
                    {
                        return false;
                    }
            }
        }

        public bool IsBoolean()
        {
            switch (mDbType)
            {
                case DbType.Boolean:
                    {
                        return true;
                    }

                default:
                    {
                        return false;
                    }
            }
        }

        public bool _IsString()
        {
            switch (mDbType)
            {
                case DbType.AnsiString or DbType.AnsiStringFixedLength or DbType.String or DbType.StringFixedLength:
                    {
                        return true;
                    }

                default:
                    {
                        return false;
                    }
            }
        }

        public bool IsString()
        {
            switch (mDbType)
            {
                case DbType.AnsiString or DbType.AnsiStringFixedLength or DbType.String or DbType.StringFixedLength:
                    {
                        return true;
                    }

                default:
                    {
                        return false;
                    }
            }
        }

        public bool IsChanged()
        {
            //if (Operators.ConditionalCompareObjectNotEqual(mValue, mOriginalValue, false))
            //if (mValue!=mOriginalValue)
            if (mValue.Equals(mOriginalValue) == false)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public bool IsEdited()
        {
            return CheckForDataChange(true);
        }

        public bool IsNumeric()
        {
            switch (mDbType)
            {
                case DbType.Currency or DbType.Decimal or DbType.Double or DbType.Int16 or DbType.Int32 or DbType.Int64 or DbType.Single or DbType.VarNumeric:
                    {
                        return true;
                    }

                default:
                    {
                        return false;
                    }
            }
        }

        private string SetDBColumnNameE(string ColumnName)
        {
            if (ColumnName.StartsWith("[") & ColumnName.EndsWith("]"))
            {
                //return Strings.Mid(ColumnName, 2, Strings.Len(ColumnName) - 2);
                return ColumnName.Substring(1, ColumnName.Length - 2);

            }

            if (ColumnName.StartsWith("\"") & ColumnName.EndsWith("\""))
            {
                //return Strings.Mid(ColumnName, 2, Strings.Len(ColumnName) - 2);
                return ColumnName.Substring(1, ColumnName.Length - 2);

            }

            return ColumnName;
        }

        public DbColumn()
        {
            mTableName = TableName;
            BoundControls.DbColumn = this;
        }

        public DbColumn(string DbColumnName, string DbColumnNameAlias)
        {
            mDbColumnName = DbColumnName;
            mDbColumnNameAlias = DbColumnNameAlias;
            mDbColumnNameE = SetDBColumnNameE(DbColumnName);
            mTableName = TableName;
            BoundControls.DbColumn = this;
        }

        public DbColumn(string DbColumnName, DbType DbType, bool IsPrimaryKey, object DefaultValue, int Size = 0, DefaultValueEvaluationMode DefaultValueEvaluationMode = DefaultValueEvaluationMode.OnEntityCreation)
        {
            mTableName = TableName;
            mDbColumnName = DbColumnName;
            mDbColumnNameE = SetDBColumnNameE(DbColumnName);
            mDbType = DbType;
            switch (DbType)
            {
                case DbType.AnsiString or DbType.AnsiStringFixedLength or DbType.String or DbType.StringFixedLength:
                    {
                        this.Size = Size;
                        break;
                    }

                case DbType.Date:
                    {
                        DateTimeResolution = DateTimeResolution.Day;
                        break;
                    }

                case DbType.DateTime:
                    {
                        DateTimeResolution = DateTimeResolution.Second;
                        break;
                    }

                case DbType.DateTime2:
                    {
                        DateTimeResolution = DateTimeResolution.Millisecond;
                        break;
                    }

                case DbType.Time:
                    {
                        DateTimeResolution = DateTimeResolution.Millisecond;
                        break;
                    }

                default:
                    {
                        break;
                    }
            }

            mIsPrimaryKey = IsPrimaryKey;
            mDefaultValue = DefaultValue;
            BoundControls.DbColumn = this;
            this.DefaultValueEvaluationMode = DefaultValueEvaluationMode;
        }

        public DbColumn(string DbColumnName, string FriendlyName, DbType DbType, bool IsPrimaryKey = false, object DefaultValue = null, int Size = 0, DefaultValueEvaluationMode DefaultValueEvaluationMode = DefaultValueEvaluationMode.OnEntityCreation)
        {
            mTableName = TableName;
            mDbColumnName = DbColumnName;
            mDbColumnNameE = SetDBColumnNameE(DbColumnName);
            mFriendlyName = FriendlyName;
            mDbType = DbType;
            switch (DbType)
            {
                case DbType.AnsiString or DbType.AnsiStringFixedLength or DbType.String or DbType.StringFixedLength:

                    {
                        this.Size = Size;
                        break;
                    }

                case DbType.Date:
                    {
                        DateTimeResolution = DateTimeResolution.Day;
                        break;
                    }

                case DbType.DateTime:
                    {
                        DateTimeResolution = DateTimeResolution.Second;
                        break;
                    }

                case DbType.DateTime2:
                    {
                        DateTimeResolution = DateTimeResolution.Millisecond;
                        break;
                    }

                case DbType.Time:
                    {
                        DateTimeResolution = DateTimeResolution.Millisecond;
                        break;
                    }

                default:
                    {
                        break;
                    }
            }

            this.DefaultValueEvaluationMode = DefaultValueEvaluationMode;
            mIsPrimaryKey = IsPrimaryKey;
            mDefaultValue = DefaultValue;
            BoundControls.DbColumn = this;
        }

        public DbColumn(string DbCOlumnName, string FriendlyName, DbType DbType, bool IsPrimaryKey = false, int Size = 0, bool IsIdentity = false, object IdentitySeed = null, object IdentityIncrement = null)
        {
            mTableName = TableName;
            mDbColumnName = DbCOlumnName;
            mDbColumnNameE = SetDBColumnNameE(DbCOlumnName);
            mDbType = DbType;
            switch (DbType)
            {
                case DbType.AnsiString or DbType.AnsiStringFixedLength or DbType.String or DbType.StringFixedLength:

                    {
                        this.Size = Size;
                        break;
                    }

                case DbType.Date:
                    {
                        DateTimeResolution = DateTimeResolution.Day;
                        break;
                    }

                case DbType.DateTime:
                    {
                        DateTimeResolution = DateTimeResolution.Second;
                        break;
                    }

                case DbType.DateTime2:
                    {
                        DateTimeResolution = DateTimeResolution.Millisecond;
                        break;
                    }

                case DbType.Time:
                    {
                        DateTimeResolution = DateTimeResolution.Millisecond;
                        break;
                    }

                default:
                    {
                        break;
                    }
            }

            mIsPrimaryKey = IsPrimaryKey;
            mFriendlyName = FriendlyName;
            mIsIdentity = IsIdentity;
            BoundControls.DbColumn = this;
        }


        public void DataBindings(ref dynamic Control, string PropertyName, DataTable DataTable, bool Clear)
        {
            try
            {
                Control.DataBindings.Add(PropertyName, DataTable, mDbColumnName);
            }
            catch
            {
            }
        }

        public void DataBindings(ref dynamic Control, string PropertyName, DataTable DataTable)
        {
            DataBindings(ref Control, PropertyName, DataTable, false);
        }

        public void DataBindingsClear(ref dynamic Control)
        {
            Control.DataBindings.Clear();
        }

        public bool CheckForDataChange(dynamic OnlyBoundControls = null)
        {
            // verifica per DbColumn con bound
            ExecutionResult.Context = "DbColumn:CheckForDataChange";
            mDataChanged = false;
            mValidated = false;
            object EditedValue = null;
            object DBValue = null;
            int i = 0;
            switch (mDbObject.DataBinding)
            {
                case DataBoundControlsBehaviour.BasicDALDataBinding:
                    {
                        // verifica per DbColumn senza bound
                        if (mBoundControls.Count == 0)
                        {
                            if (mDataChanged)
                                i = i + 1;
                        }
                        else
                        {
                            // verifica per DbColumn con bound
                            foreach (BoundControl BoundControl in mBoundControls)
                            {
                                BoundControl.IsEdited = false;
                                switch (BoundControl.BindingBehaviour)
                                {
                                    case BindingBehaviour.ReadWrite or BindingBehaviour.ReadWriteAddNew:
                                        {
                                            //EditedValue = Interaction.CallByName(BoundControl.Control, BoundControl.PropertyName, CallType.Get);
                                            EditedValue = Interaction.CallByName(BoundControl.Control, BoundControl.PropertyName, CallType.Get);
                                            //EditedValue = BasicDalSharedCode.Cast(EditedValue, mDbType, DbObject.NullDateValue, mDateTimeResolution);
                                            EditedValue = Utilities.DbValueCast(EditedValue, mDbType, DbObject.NullDateValue, mDateTimeResolution);




                                            switch (mDbObject.DbConfig.Provider)
                                            {

                                                case Providers.OracleDataAccess:
#if COMPILEORACLE
                                                    //DBValue = BasicDalSharedCode.Cast(mDbObject.CurrentDataRow[mDbColumnNameE],DbType);
                                                    DBValue = Utilities.DbValueCast(mDbObject.CurrentDataRow[mDbColumnNameE], DbType);
#endif

                                                    break;

                                                default:
                                                    {
                                                        DBValue = mDbObject.CurrentDataRow[mDbColumnNameE];
                                                        break;
                                                    }
                                            }

                                            break;
                                        }

                                    default:
                                        {
                                            break;
                                        }
                                }
                                // Bound or unbound fields
                                if (!ReferenceEquals(DBValue, DBNull.Value) & !ReferenceEquals(EditedValue, DBNull.Value))
                                {
                                    switch (mDbType)
                                    {
                                        case DbType.Date:
                                        case DbType.DateTime:
                                        case DbType.DateTime2:
                                        case DbType.Time:
                                        case DbType.DateTimeOffset:
                                            {
                                                //DBValue = BasicDalSharedCode.TruncateDateTime(Convert.ToDateTime (DBValue), mDateTimeResolution);
                                                //EditedValue = BasicDalSharedCode.TruncateDateTime(Convert.ToDateTime(EditedValue), mDateTimeResolution);
                                                DBValue = Utilities.TruncateDateTime(Convert.ToDateTime(DBValue), mDateTimeResolution);
                                                EditedValue = Utilities.TruncateDateTime(Convert.ToDateTime(EditedValue), mDateTimeResolution);
                                                break;
                                            }

                                        default:
                                            {
                                                break;
                                            }
                                    }

                                    //if (Conversions.ToBoolean(Operators.ConditionalCompareObjectNotEqual(DBValue, EditedValue, false)))
                                    if (DBValue.Equals(EditedValue) == false)
                                    {
                                        mDataChanged = true;
                                        BoundControl.IsEdited = true;
                                        i = i + 1;
                                        break;
                                    }
                                }

                                if (EditedValue is null)
                                {
                                    EditedValue = "";
                                }

                                if (ReferenceEquals(DBValue, DBNull.Value) & !string.IsNullOrEmpty(EditedValue.ToString()))
                                {
                                    // If (DBValue Is DBNull.Value And EditedValue <> vbNull) Then
                                    switch (mDbType)
                                    {
                                        case DbType.Date:
                                        case DbType.DateTime:
                                        case DbType.DateTime2:
                                        case DbType.Time:
                                        case DbType.DateTimeOffset:
                                            {
                                                DBValue = Utilities.TruncateDateTime(Convert.ToDateTime(DBValue), mDateTimeResolution);
                                                EditedValue = Utilities.TruncateDateTime(Convert.ToDateTime(EditedValue), mDateTimeResolution);
                                                //if (Conversions.ToBoolean(Operators.ConditionalCompareObjectEqual(EditedValue, DbObject.NullDateValue, false)))
                                                if (EditedValue.Equals(DbObject.NullDateValue) == false)
                                                {
                                                    EditedValue = DBNull.Value;
                                                }

                                                break;
                                            }

                                        default:
                                            {
                                                break;
                                            }
                                    }

                                    //if (Conversions.ToBoolean(Operators.ConditionalCompareObjectNotEqual(DBValue.ToString(), EditedValue, false)))
                                    if (DBValue.ToString().Equals(EditedValue) == false)
                                    {
                                        mDataChanged = true;
                                        BoundControl.IsEdited = true;
                                        i = i + 1;
                                        break;
                                    }
                                }
                            }
                        }

                        break;
                    }

                case DataBoundControlsBehaviour.WindowsFormsDataBinding:
                    {
                        //if (Conversions.ToBoolean(Operators.ConditionalCompareObjectEqual(OnlyBoundControls, false, false)))
                        if (OnlyBoundControls == false)
                        {
                            if (mDataChanged)
                                i = i + 1;
                        }

                        break;
                    }

                case DataBoundControlsBehaviour.NoDataBinding:
                    {
                        //if (Conversions.ToBoolean(Operators.ConditionalCompareObjectEqual(OnlyBoundControls, false, false)))
                        if (OnlyBoundControls == false)
                        {
                            if (mDataChanged)
                                i = i + 1;
                        }

                        break;
                    }
            }

            if (mDataChanged == true)
            {
                mValidated = false;
            }

            return mDataChanged;
        }

        public object GetMaxValue()
        {
            object x;
            string SQL;
            SQL = "SELECT MAX(" + DbColumnName + ") FROM " + DbObject.DbTableName;
            x = DbObject.ExecuteScalar(SQL);
            if (x == DBNull.Value)
            {
                switch (DbType)
                {
                    case DbType.Currency:
                    case DbType.Decimal:
                    case DbType.Double:
                    case DbType.Int16:
                    case DbType.Int32:
                    case DbType.Int64:
                    case DbType.Single:
                    case DbType.UInt16:
                    case DbType.UInt32:
                    case DbType.UInt64:
                        {
                            x = 0;
                            break;
                        }

                    default:
                        {
                            x = "";
                            break;
                        }
                }
            }

            return x;
        }
    }
    #endregion

    [Serializable()]
    public class DbOrderBy : ArrayList
    {
        public void AddDbColumn(BasicDAL.DbColumn DbColumn, OrderBySortDirection SortDirection)
        {
            string Seq = " ASC ";
            switch (SortDirection)
            {
                case OrderBySortDirection.Ascendig:
                    {
                        Seq = " ASC ";
                        break;
                    }

                case OrderBySortDirection.Descending:
                    {
                        Seq = " DESC ";
                        break;
                    }

                default:
                    {
                        break;
                    }
            }

            Add(" " + DbColumn.DbColumnName  + Seq);
        }

        public string BuildOrderByClause()
        {
            string oc = "";
            foreach (string  x in this)
                oc = oc + x  + ",";
            if (oc.Length > 1)
                oc = oc.Substring(0, oc.Length - 1);
            return oc;
        }
    }

    #region DbObject Class
    [Serializable]
    public class DbObject : IDisposable
    {
        public ArrayList xDbColumns = new ArrayList();

        private Dictionary<int, string> mCommandParametersSourceColumns = new Dictionary<int, string>();
        private Dictionary<int, string> mUpdateParametersSourceColumns = new Dictionary<int, string>();
        private Dictionary<int, string> mInsertParmetersSourceColumns = new Dictionary<int, string>();
        private Dictionary<int, string> mDeleteParmetersSourceColumns = new Dictionary<int, string>();
        private Dictionary<int, string> mExecuteParametersSourceColumns = new Dictionary<int, string>();
        private DbDataAdapter _objAdapter;
        // MOST IMPORTANT OBJECTS
        private DbColumns mDbColumns;
        private DbParameters mDbParameters;
        public int CommandTimeOut = 600;
        public string WithOptions = "";
        public string AppendedSQL = "";
        //public DbConnection CachedConnection;
        public bool UseDataReader = false;
        public bool DisableEvents = false;
        public DataRow CurrentDataRow = null;
        public int BatchRows = 100;
        public bool UpdateBatchEnabled = false;
        public ExecutionResult ExecutionResult = new ExecutionResult();
        public RuntimeUI RuntimeUI = RuntimeUI.WindowsForms;
        public dynamic ValidatorLabelControl = null;
        public bool UseParallelism = false;
        private DbFiltersGroup objFiltersGroup = new DbFiltersGroup();
        private string mFriendlyName = "";
        private string mDbTableName = "";
        private FieldInfo[] mFields;
        private FieldInfo mField;
        private PropertyInfo[] mProperties;
        private PropertyInfo mProperty;
        private InterfaceModeEnum mInterfaceMode;
        private bool mSQLUpdateCache = false;
        private DbProviderFactory objFactory = null;
        private DbConnection objConnection;
        private DbCommand objCommand;
        private DbCommand objInsertCommand;
        private DbCommand objUpdateCommand;
        private DbCommand objDeleteCommand;
        private DbCommand objExecuteCommand;
        private DbCommand objStoredProcedureCommand;
        private string mSQLQuery = "";
        private int mBatchRows = 0;
        private object mCurrencyManager;
        private DataBoundControlsBehaviour mDataBinding;
        private bool mBindingBehaviour = false;
        private int mCurrentPosition = -1;
        private bool mAddNewStatus = false;
        private bool mHandleErrors = true;
        private string mLastError = "";
        private int mLastErrorCode = 0;
        private string mLastErrorComplete = "";
        private Exception mLasteException = null;
        private DbColumn mIdentityDBColumn = null;
        private DbCommand objDataReaderCommand;
        private DbConnection objDataReaderConnection;
        private DbCommandBuilder objCommandBuilder;
        private string SQLUpdate = "";
        private string SQLInsert = "";
        private string SQLDelete = "";
        private string SQLSelect = "";
        private string SQLWhere = "";
        private bool mDbObjectInitialized = false;
        private DbConnection mCachedConnection;
        private DbCommand mCachedCommand;
        private string mOrderBy = "";
        private Field[] Fields;
        private Field[] Parameters;
        private Join[] Joins;
        private string FieldsList = " * ";
        private string ParameterList = " * ";
        private bool mDataChanged = false;
        private DbObjectTypeEnum mDbObjectType = DbObjectTypeEnum.Table;
        private int mRowCount = 0;
        private int mRowsGroups = 0;
        private int mRowsLimit = 0;
        private bool mBOF = false;
        private bool mEOF = false;
        private DbObjects mDbObjects = new DbObjects();
        private DataTable mInMemoryJoinDataTable;
        private int mCurrentRowGroup = 0;
        private int mCurrentRowsGroup = 0;
        private DataTable mSchemaTable = new DataTable();
        private int mTopRecords = 0;
        private DbConfig mDbConfig = new DbConfig();
        private bool mSuppressErrorsNotification = false;
        private object mRedirectErrorsNotificationTo = null;
        private bool mErrorState = false;
        private string mConnectionString;
        private bool mIsReadOnly = false;
        private bool mDistinctClause = false;
        private bool mOnlyEntityInitialization = true;
        private DateTime mNullDateValue = System.DateTime.Parse("01/01/0001 12:00:00 AM");
        private List<DbColumn> mDbColumnsWithValidator = new List<DbColumn>();
        private DbTransaction mDbTransaction = null;
        private DbACL mACL = new DbACL();
        private DbAuditing DbAuditing = new DbAuditing() ;
        private DbOrderBy mDbOrderBy = new DbOrderBy();




        public struct Field
        {
            public string Name;
            public string TypeName;
            public Type Type;
            public int Index;
            public string DbColumnName;
            public string DbColumnNameE;
            public string DbColumnNameAlias;
        }

        public struct Parameter
        {
            public string Name;
            public string TypeName;
            public Type Type;
            public int Index;
            public string ParameterName;
            public string ParameterNameE;
            public string ParameterNameAlias;
        }

        public struct Join
        {
            public string Name;
            public DbJoin DbJoin;
        }


        public event WriteCSVRowEventHandler WriteCSVRow;

        public delegate void WriteCSVRowEventHandler(int rownum, ref bool Cancel);

        public event BoundCompletedEventHandler BoundCompleted;

        public delegate void BoundCompletedEventHandler();

        public event DataEventAfterEventHandler DataEventAfter;

        public delegate void DataEventAfterEventHandler(DataEventType EventType);

        public event DataEventBeforeEventHandler DataEventBefore;

        public delegate void DataEventBeforeEventHandler(DataEventType EventType, ref bool Cancel);

        public DbObject(string Name =null)
        {
            if(Name==null)
            {
                Name = System.Guid.NewGuid().ToString();
            }
            mName = Name;
        }


        public virtual DbDataAdapter objAdapter
        {
            [MethodImpl(MethodImplOptions.Synchronized)]
            get
            {
                return _objAdapter;
            }

            [MethodImpl(MethodImplOptions.Synchronized)]
            set
            {
                _objAdapter = value;
            }
        }

        public DbOrderBy DbOrderBy
        {
            get
            {
                return mDbOrderBy;
            }
            set
            {
                mDbOrderBy = value;
            }
        }

        private bool mLogError;
        public bool LogError
        {
            get
            {
                return mLogError;
            }
            set
            {
                mLogError = value;
            }
        }

        private string mLogFile;
        public string LogFile
        {
            get
            {
                return mLogFile;
            }
            set
            {
                mLogFile = value;
            }
        }

        private DataTable _objDataTable= new DataTable();

        public virtual DataTable objDataTable
        {
            [MethodImpl(MethodImplOptions.Synchronized)]
            get
            {
                return _objDataTable;
            }

            [MethodImpl(MethodImplOptions.Synchronized)]
            set
            {
                _objDataTable = value;
            }
        }

        private DbDataReader _objDataReader;

        public virtual DbDataReader objDataReader
        {
            [MethodImpl(MethodImplOptions.Synchronized)]
            get
            {
                return _objDataReader;
            }

            [MethodImpl(MethodImplOptions.Synchronized)]
            set
            {
                _objDataReader = value;
            }
        }

        private DataSet _objDataSet;

        public virtual DataSet objDataSet
        {
            [MethodImpl(MethodImplOptions.Synchronized)]
            get
            {
                return _objDataSet;
            }

            [MethodImpl(MethodImplOptions.Synchronized)]
            set
            {
                _objDataSet = value;
            }
        }

        public DbColumns DbColumns
        {
            get
            {
                if (mDbColumns is null)
                {
                    mDbColumns = GetDbColumnsE();
                }

                return mDbColumns;
            }

            set
            {
                mDbColumns = value;
            }
        }

        public DbParameters DbParameters
        {
            get
            {
                if (mDbParameters is null)
                {
                    mDbParameters = GetDbParameters();
                }

                return mDbParameters;
            }

            set
            {
                mDbParameters = value;
            }
        }

        public DbACL ACL
        {
            get
            {
                return mACL;
            }

            set
            {
                mACL = value;
            }
        }

        public List<T> ToList<T>() where T : class, new()
        {
            try
            {
                var list = new List<T>();
                var proplist = new List<PropertyInfo>();
                var _obj = new T();
                foreach (PropertyInfo prop in _obj.GetType().GetProperties())
                    // MsgBox(prop.Name & " - " & prop.PropertyType.ToString())
                    proplist.Add(prop);
                object safevalue = null;
                Type propType;
                var obj = new T();
                foreach (DataRow row in DataTable.AsEnumerable())
                {
                  
                    
                    obj = new T();
                    foreach (PropertyInfo prop in proplist)
                    {
                        try
                        {
                            propType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                            safevalue = null;
                            if (row[prop.Name] is DBNull == false)
                            {
                                safevalue = row[prop.Name] is null ? null : Convert.ChangeType(row[prop.Name], propType);
                            }

                            prop.SetValue(obj, safevalue, null);
                        }
                        catch  (Exception x)
                        {
                            continue;
                        }
                    }

                    list.Add(obj);
                }

                return list;
            }
            catch
            {
                return null;
            }
        }

        public bool FromList<T>(List<T> List) where T : class, new()
        {
            //if (this.objDataTable  is null)
            //{
            //    return false;
            //}

            if (List is null == true)
            {
                return false;
            }
            // Obtains the properties definition of the generic class.
            // With this, we are going to know the property names of the class
            var _TypeT = typeof(T);
            var defaultInstance = Activator.CreateInstance(_TypeT);
            var pi = _TypeT.GetProperties();
            var fi = _TypeT.GetFields();
            DataRow row;
            GetEmptyDataTable();
            this.objDataTable.Rows.Clear(); 
            
            object columnvalue = null;
            foreach (var myclass in List)
            {
                row = objDataTable.NewRow();
                foreach (PropertyInfo prop in pi)
                {
                    try
                    {
                        // Get the value of the row according to the field name
                        // Remember that the classïs members and the tableïs field names
                        // must be identical
                        columnvalue = Interaction.CallByName(myclass, prop.Name, CallType.Get);
                        // Know check if the value is null. 
                        // If not, it will be added to the instance
                        if (!ReferenceEquals(columnvalue, DBNull.Value))
                        {
                            row[prop.Name] = columnvalue;
                        }
                    }
                    catch
                    {
                    }
                }

                foreach (FieldInfo prop in fi)
                {
                    try
                    {
                        // Get the value of the row according to the field name
                        // Remember that the classïs members and the tableïs field names
                        // must be identical

                        columnvalue = Interaction.CallByName(myclass, prop.Name, CallType.Get);
                        //columnvalue = Utilities.CallByName(myclass, prop.Name, Utilities.CallType.Get);

                        // Know check if the value is null. 
                        // If not, it will be added to the instance
                        if (!ReferenceEquals(columnvalue, DBNull.Value))
                        {
                            if (!ReferenceEquals(columnvalue, DBNull.Value))
                            {
                                row[prop.Name] = columnvalue;
                            }
                        }
                    }
                    catch
                    {
                    }
                }

                objDataTable.Rows.Add(row);
            }

            this.objDataTable.AcceptChanges();
            mRowCount = this.objDataTable.Rows.Count;
            mCurrentPosition = 0;
            mBOF = false;
            DataRowGetValues(this.objDataTable .Rows [0], mDataBinding);
            //MoveFirst();
            return true;
        }

        public bool FromObject<T>(T _T) where T : class, new()
        {
            ExecutionResult.Reset();
            ExecutionResult.Context = "DbObject:FromObject";
            try
            {
                var proplist = new List<PropertyInfo>();
                foreach (PropertyInfo prop in _T.GetType().GetProperties())
                    proplist.Add(prop);
                UnloadAll();
                var row = objDataTable.NewRow();
                // Dim obj As New T()
                Type propType;
                object safevalue;
                foreach (PropertyInfo prop in proplist)
                {
                    try
                    {
                        propType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                        safevalue = null;
                        safevalue = prop.GetValue(_T, null);
                        row[prop.Name] = safevalue;
                    }
                    catch
                    {
                        continue;
                    }
                }

                CurrentDataRow = row;
                mCurrentPosition = 1;
                DataRowGetValues(row, mDataBinding);
                return true;
            }
            catch (Exception Ex)
            {


                HandleExceptions(Ex, ExecutionResult.Context);
                return false;
            }
        }

        public T ToObject<T>() where T : class, new()
        {
            ExecutionResult.Reset();
            ExecutionResult.Context = "DbObject:ToObject";


            try
            {
                var _T = new T();
                var proplist = new List<PropertyInfo>();
                foreach (PropertyInfo prop in _T.GetType().GetProperties())
                    proplist.Add(prop);

                if (CurrentDataRow == null)
                    return null;

                var row = CurrentDataRow;

                foreach (PropertyInfo prop in proplist)
                {
                    try
                    {
                        object propType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                        object safevalue = null;
                        if (row[prop.Name] is DBNull == false)
                        {
                            safevalue = row[prop.Name] is null ? null : Convert.ChangeType(row[prop.Name], (TypeCode)propType);
                        }
                        prop.SetValue(_T, safevalue, null);
                    }
                    catch
                    {
                        continue;
                    }
                }

                return _T; // obj
            }
            catch (Exception Ex)
            {
                HandleExceptions(Ex, ExecutionResult.Context);
                return null;
            }
        }

        public DateTime NullDateValue
        {
            get
            {
                return mNullDateValue;
            }

            set
            {
                mNullDateValue = value;
            }
        }

        public DbProviderFactory DbProviderFactory
        {
            get
            {
                return objFactory;
            }
        }

        public string FriendlyName
        {
            get
            {
                string FriendlyNameRet = default;
                if (string.IsNullOrEmpty(mFriendlyName.Trim()))
                {
                    mFriendlyName = mDbTableName;
                }

                FriendlyNameRet = mFriendlyName;
                return FriendlyNameRet;
            }

            set
            {
                mFriendlyName = value;
            }
        }

        public bool SQLUpdateCache
        {
            get
            {
                return mSQLUpdateCache;
            }

            set
            {
                mSQLUpdateCache = value;
            }
        }

        public DbTransaction DbTransaction
        {
            get
            {
                return mDbTransaction;
            }

            set
            {
                mDbTransaction = DbTransaction;
                objCommand.Transaction = DbTransaction;
                objUpdateCommand.Transaction = DbTransaction;
                objInsertCommand.Transaction = DbTransaction;
                objDeleteCommand.Transaction = DbTransaction;
            }
        }

        public System.Data.Common.DbParameter AddStoredProcedureParameter(string ParameterName, DbType DbType, object Value = null)
        {
            var parameter = objFactory.CreateParameter();
            parameter.ParameterName = DbConfig.ParameterNamePrefix + ParameterName;
            parameter.DbType = DbType;
            parameter.Value = Value;
            objStoredProcedureCommand.Parameters.Add(parameter);
            return parameter;
        }

        public int objRowsCount()
        {
            string sqlCount = "SELECT COUNT(*)  as RowsCount FROM " + DbTableName;
            int RowCount;
            if (DbObjectInitialized)
            {
                string SQLWhere = "";
                DbCommand argDBCommand = objCommand;
                FiltersGroup.BuildSQLFilter(ref SQLWhere, ref argDBCommand, DbConfig.Provider);
                objCommand = (DbCommand)argDBCommand;
                sqlCount = sqlCount + " WHERE " + SQLWhere;
                RowCount = Convert.ToInt32(ExecuteScalar(sqlCount, objCommand.Parameters, CommandType.Text, ConnectionState.CloseOnExit));
                if (!string.IsNullOrEmpty(LastError))
                {
                    return -1;
                }
                else
                {
                    return RowCount;
                }
            }
            else
            {
                return -1;
            }
        }

        public void CaseSensitiveQuery(bool State)
        {
            // impostazione case sensitive ORACLE

            if (DbObjectIsValid)
            {
                switch (mDbConfig.Provider)
                {

                    case Providers.OracleDataAccess:
#if COMPILEORACLE
                        this.objCommand.CommandText = "ALTER SESSION SET NLS_COMP=LINGUISTIC";
                        this.objCommand.CommandType = CommandType.Text;
                        this.objCommand.ExecuteNonQuery();
                        if (State == true)
                        {
                            this.objCommand.CommandText = "ALTER SESSION SET NLS_SORT=BINARY_CI";
                        }
                        else
                        {
                            this.objCommand.CommandText = "ALTER SESSION SET NLS_SORT=BINARY";
                        }
                        this.objCommand.ExecuteNonQuery();
#endif 
                        break;

                    default:
                        {
                            break;
                        }
                }
            }
        }

        public string DumpRow()
        {
            StringBuilder Dump = new StringBuilder();
            string vbCrLf = "\r\n";
            Dump.Append("DBObject RowDump for " + ToString() + " - DBTableName = " + DbTableName + vbCrLf);
            Dump.Append("------------------------------------------------------------------------------" + vbCrLf);
            foreach (DbColumn DbColumn in mDbColumns)
            {
                Dump.Append("! " + String.Format("000", DbColumn.ColumnOrdinal) + " " + DbColumn.DbColumnName + " = " + DbColumn.Value.ToString() + vbCrLf);
            }
            Dump.Append("------------------------------------------------------------------------------" + vbCrLf);
            return Dump.ToString();
        }

        public string GetCurrentRowAsCSV(bool FirstRowWithColumnName)
        {
            string vbCrLf = "\r\n";

            DataTable nt = objDataTable.Clone();
            foreach (DbColumn dbc in mDbColumns)
                nt.Columns[dbc.DbColumnNameE].DefaultValue = dbc.DefaultValue;
            nt.ImportRow(CurrentDataRow);
            //bool Cancel = false;
            //int CsvRowNumber = 0;


            // first write a line with the columns name
            string sep = ",";
            var builder1 = new StringBuilder();
            if (FirstRowWithColumnName == true)
            {
                foreach (DbColumn col in mDbColumns)
                    builder1.Append(sep).Append(Utilities.QuoteValue(col.DbColumnNameE));
            }

            var builder2 = new StringBuilder();
            foreach (DataRow row in nt.Rows)
            {
                foreach (DataColumn col in nt.Columns)
                {
                    string value = "";
                    if (row[col.ColumnName] is DBNull == false)
                    {
                        value = Utilities.QuoteValue(Convert.ToString(row[col.ColumnName]));
                    }

                    builder2.Append(sep).Append(value);
                }
            }

            return builder1.ToString() + vbCrLf + builder2.ToString();
        }

        public string GetCurrentRowAsXML(bool IncludeSchema)
        {
            var nt = objDataTable.Clone();
            foreach (DbColumn dbc in mDbColumns)
                nt.Columns[dbc.DbColumnNameE].DefaultValue = dbc.DefaultValue;
            nt.ImportRow(CurrentDataRow);
            var str = new MemoryStream();
            switch (IncludeSchema)
            {
                case true:
                    {
                        nt.WriteXml(str, XmlWriteMode.WriteSchema);
                        break;
                    }

                case false:
                    {
                        nt.WriteXml(str);
                        break;
                    }
            }

            str.Seek(0L, SeekOrigin.Begin);
            var sr = new StreamReader(str);
            string xmlstr;
            xmlstr = sr.ReadToEnd();
            var xml = new System.Xml.XmlDocument();
            xml.LoadXml(xmlstr);
            // xmlstr = xml.ChildNodes(0).InnerXml.ToString
            // xmlstr = xmlstr.Replace(xml.ChildNodes(0).Name, "DATATABLE")

            return xml.InnerXml;
        }

        public string GetCurrentRowHash(HashProviders HashProvider = HashProviders.MD5)
        {
            StringBuilder sb = new StringBuilder();
            foreach (string s in objDataTable.Rows[(int)mCurrentPosition].ItemArray)
            {
                try
                {
                    sb.Append(s);
                }
                catch
                {
                }
            }

            return Utilities.CryptographyHelper.GetHashValue(sb.ToString(), HashProvider);
        }

        private string mName;
        public string Name
        {
            get
            {

                return mName;
            }
            set
            {
                mName = value;
            }
        }

        //public ParameterStyles ParametersStyle
        //{
        //    get
        //    {
        //        return mParametersStyle;
        //    }

        //    set
        //    {
        //        mParametersStyle = value;
        //    }
        //}

        public DbConnection DbConnection
        {
            get
            {
                return objConnection;
            }

            set
            {
                objConnection = value;
            }
        }

        public DbConnection Connection
        {
            get
            {
                return objConnection;
            }

            set
            {
                objConnection = value;
            }
        }

        //public bool SupportSquareBrackets
        //{
        //    get
        //    {
        //        return mSupportSquareBrackets;
        //    }

        //    set
        //    {
        //        mSupportSquareBrackets = value;
        //    }
        //}

        //public bool RequireQuotedTableName
        //{
        //    get
        //    {
        //        return mRequireQuotedTableName;
        //    }

        //    set
        //    {
        //        mRequireQuotedTableName = value;
        //    }
        //}

        //public bool SupportTopRecords
        //{
        //    get
        //    {
        //        return mSupportTopRecords;
        //    }

        //    set
        //    {
        //        mSupportTopRecords = value;
        //    }
        //}

        public DbColumn IdentityDBColumn
        {
            get
            {
                return mIdentityDBColumn;
            }

            set
            {
                mIdentityDBColumn = value;
            }
        }

        public bool OnlyEntityInitialization
        {
            get
            {
                return mOnlyEntityInitialization;
            }

            set
            {
                mOnlyEntityInitialization = value;
            }
        }

        public bool DistinctClause
        {
            get
            {
                return mDistinctClause;
            }

            set
            {
                mDistinctClause = value;
            }
        }

        public bool IsReadOnly
        {
            get
            {
                return mIsReadOnly;
            }

            set
            {
                mIsReadOnly = value;
            }
        }

        public bool ErrorState
        {
            get
            {
                return mErrorState;
            }

            set
            {
                mErrorState = value;
            }
        }

        public bool SuppressErrorsNotification
        {
            get
            {
                bool SuppressErrorsNotificationRet = default;
                SuppressErrorsNotificationRet = mSuppressErrorsNotification;
                return SuppressErrorsNotificationRet;
            }

            set
            {
                mSuppressErrorsNotification = value;
            }
        }

        public object RedirectErrorsNotificationTo
        {
            get
            {
                object RedirectErrorsNotificationToRet = default;
                RedirectErrorsNotificationToRet = mRedirectErrorsNotificationTo;
                return RedirectErrorsNotificationToRet;
            }

            set
            {
                mRedirectErrorsNotificationTo = value;
            }
        }

        public string SQLQuery
        {
            get
            {
                string SQLQueryRet = default;
                SQLQueryRet = mSQLQuery;
                return SQLQueryRet;
            }

            set
            {
                mSQLQuery = value;
            }
        }

        public int TopRecords
        {
            get
            {
                int TopRecordsRet = default;
                TopRecordsRet = mTopRecords;
                return TopRecordsRet;
            }

            set
            {
                mTopRecords = value;
            }
        }

        public int RowsLimit
        {
            get
            {
                int RowsLimitRet = default;
                RowsLimitRet = mRowsLimit;
                return RowsLimitRet;
            }

            set
            {
                mRowsLimit = value;
            }
        }

        public DbConfig DbConfig
        {
            get
            {
                DbConfig DbConfigRet = default;
                DbConfigRet = mDbConfig;
                return DbConfigRet;
            }

            set
            {
                mDbConfig = value;
                this.DbAuditing.SetDbObject(this);

            }
        }

        public int CurrentPosition
        {
            get
            {
                int CurrentPositionRet = default;
                CurrentPositionRet = (int)mCurrentPosition;
                return CurrentPositionRet;
            }
        }

        public int CurrentRowsGroup
        {
            get
            {
                int CurrentRowsGroupRet = default;
                CurrentRowsGroupRet = mCurrentRowsGroup;
                return CurrentRowsGroupRet;
            }

            set
            {
                mCurrentRowsGroup = value;
            }
        }

        public bool DataChanged
        {
            get
            {
                bool DataChangedRet = default;
                mDataChanged = CheckForDataChange();
                DataChangedRet = mDataChanged;
                return DataChangedRet;
            }

            set
            {
                mDataChanged = value;
            }
        }

        public DbDataReader DbDataReader
        {
            get
            {
                DbDataReader DbDataReaderRet = default;
                DbDataReaderRet = objDataReader;
                return DbDataReaderRet;
            }

            set
            {
                objDataReader = value;
            }
        }

        public DataTable DataTable
        {
            get
            {
                DataTable DataTableRet = default;
                DataTableRet = objDataTable;
                return DataTableRet;
            }

            set
            {
                objDataTable = value;
            }
        }

        public DataSet DataSet
        {
            get
            {
                DataSet DataSetRet = default;
                DataSetRet = objDataSet;
                return DataSetRet;
            }

            set
            {
                objDataSet = value;
            }
        }

        public int RowsGroups
        {
            get
            {
                int RowsGroupsRet = default;
                RowsGroupsRet = mRowsGroups;
                return RowsGroupsRet;
            }

            set
            {
                mRowsGroups = value;
            }
        }

        public bool BindingBehaviour
        {
            get
            {
                bool BindingBehaviourRet = default;
                BindingBehaviourRet = mBindingBehaviour;
                return BindingBehaviourRet;
            }

            set
            {
                mBindingBehaviour = value;
            }
        }

        public DbObjects DbObjects
        {
            get
            {
                DbObjects DbObjectsRet = default;
                DbObjectsRet = mDbObjects;
                return DbObjectsRet;
            }

            set
            {
                mDbObjects = value;
            }
        }

        public bool BOF
        {
            get
            {
                bool BOFRet = default;
                BOFRet = mBOF;
                return BOFRet;
            }
        }

        public bool EOF
        {
            get
            {
                bool EOFRet = default;
                EOFRet = mEOF;
                return EOFRet;
            }
        }

        public int RowCount
        {
            get
            {
                int RowCountRet = default;
                RowCountRet = mRowCount;
                return RowCountRet;
                // RowCount = Me.objDataTable.Rows.Count
            }
        }

        public bool GetByPrimaryKey(params object[] KeyValue)
        {
            var f = new DbFilters();
            var DBPK = new DbColumns();
            DBPK = GetPrimaryKeyDbColumns();

            //For i As Integer = 0 To UBound(KeyValue, 1)
            //    f.Add(DBPK.Item(i), ComparisionOperator.Equals, KeyValue(i), LogicOperator.AND)
            //Next i


            for (int i = 0; i < KeyValue.Length; i++)
            {
                f.Add(DBPK.get_Item(i), ComparisionOperator.Equals, KeyValue[i], LogicOperator.AND);
            }


            //for (int i = 0, loopTo = Information.UBound(KeyValue, 1); i <= loopTo; i++)
            //        f.Add(DBPK.get_Item(i), ComparisionOperator.Equals, KeyValue[i], LogicOperator.AND);


            f.get_Item(f.Count - 1).LogicOperator = LogicOperator.None;
            FiltersGroup.Clear();
            FiltersGroup.Add(f);
            try
            {
                DoQuery();
                MoveFirst();
                if (mRowCount == 1)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch
            {
                return false;
            }
        }

        public bool GetByQBEValues()
        {
            return default;
            // TBD
        }

        public void ResetQBEvalues()
        {
            if (mDbColumns is null)
            {
                mDbColumns = GetDbColumns();
            }

            foreach (DbColumn dbcolumn in mDbColumns)
                dbcolumn.QBEValue = "";
        }

        public void InitBoundControls()
        {
            if (mDataBinding != DataBoundControlsBehaviour.BasicDALDataBinding)
                return;
            if (objDataTable is null)
                return;
            if (mDbColumns is null)
                return;
            bool _ReadOnly = false;
            bool RO = false;
            // Threading.Tasks.Parallel.For(0, mDbColumns.Count, Sub(_iColumn)
            // Dim _iDbColumn As BasicDAL.DbColumn = mDbColumns.Item(_iColumn)

            foreach (DbColumn _iDbColumn in mDbColumns)
            {
                foreach (BoundControl _iControl in _iDbColumn.BoundControls)
                {
                    _ReadOnly = false;
                    switch (_iControl.BindingBehaviour)
                    {
                        case BasicDAL.BindingBehaviour.ReadWriteAddNew:
                            {
                                _ReadOnly = false;
                                break;
                            }

                        case BasicDAL.BindingBehaviour.ReadAddNew:
                            {
                                if (AddNewStatus == true)
                                {
                                    _ReadOnly = false;
                                }
                                else
                                {
                                    _ReadOnly = true;
                                }

                                break;
                            }

                        case BasicDAL.BindingBehaviour.ReadWrite:
                            {
                                _ReadOnly = false;
                                break;
                            }

                        case BasicDAL.BindingBehaviour.Read:
                            {
                                _ReadOnly = true;
                                break;
                            }

                        case BasicDAL.BindingBehaviour.AddNew:
                            {
                                _ReadOnly = false;
                                break;
                            }

                        case BasicDAL.BindingBehaviour.Write:
                            {
                                _ReadOnly = false;
                                break;
                            }

                        case BasicDAL.BindingBehaviour.WriteAddNew:
                            {
                                _ReadOnly = false;
                                break;
                            }

                        default:
                            {
                                _ReadOnly = false;
                                break;
                            }
                    }

                    RO = false;
                    if (mIsReadOnly == true | _iDbColumn.IsReadOnly == true | _ReadOnly == true)
                    {
                        RO = true;
                    }

                    try
                    {
                        Interaction.CallByName(_iControl.Control, "ReadOnly", CallType.Set, RO);
                    }
                    catch
                    {
                        try
                        {
                            Interaction.CallByName(_iControl.Control, "Enabled", CallType.Set, !RO);
                        }
                        catch
                        {
                        }
                    }
                }
            }
        }

        public void UpdateBoundControls()
        {
            if (objDataTable is null)
                return;

            System.Threading.Tasks.Parallel.For(0, mDbColumns.Count, _iColumn =>
                {
                    DbColumn _dbColumn = mDbColumns.get_Item(_iColumn);
                    if (_dbColumn.DataChanged)
                    {
                        try
                        {
                            objDataTable.Rows[mCurrentPosition][_dbColumn.DbColumnNameE] = _dbColumn.Value;
                        }
                        catch
                        {
                        }
                    }
                });
            DataRowGetValues(objDataTable.Rows[mCurrentPosition], mDataBinding);
            DataChanged = false;
        }

        private string SetDBColumnNameE(string ColumnName)
        {
            if (ColumnName.StartsWith("[") & ColumnName.EndsWith("]"))
            {
                return ColumnName.Substring(1, ColumnName.Length - 2);
            }
            else
            {
                return ColumnName;
            }
        }

        public void ReadFromBoundControls()
        {
            if (objDataTable is null)
                return;

            // Dim dbColumn As BasicDAL.DbColumn
            // Dim Control As BasicDAL.BoundControl
            // If Me.objDataTable Is Nothing Then Exit Sub

            // For Each dbColumn In mDbColumns
            // For Each Control In dbColumn.BoundControls
            // Select Case Control.BindingBehaviour
            // Case Is = BasicDAL.BindingBehaviour.ReadWrite, BasicDAL.BindingBehaviour.ReadWriteAddNew

            // Try
            // CallByName(Control.Control, Control.PropertyName, CallType.Set, dbColumn.Value)
            // Catch ex As Exception

            // End Try
            // End Select
            // Next
            // Next

            System.Threading.Tasks.Parallel.For(0, mDbColumns.Count, _iColumn =>
                {
                    DbColumn _dbColumn = mDbColumns.get_Item(_iColumn); //(DbColumn)mDbColumns.ElementAtOrDefault(_iColumn);
                    foreach (BoundControl _Control in _dbColumn.BoundControls)
                    {
                        switch (_Control.BindingBehaviour)
                        {
                            case BasicDAL.BindingBehaviour.ReadWrite or BasicDAL.BindingBehaviour.ReadWriteAddNew or BasicDAL.BindingBehaviour.AddNew:
                                {
                                    try
                                    {
                                        Interaction.CallByName(_Control.Control, _Control.PropertyName, CallType.Set, _dbColumn.Value);
                                    }
                                    catch
                                    {
                                    }

                                    break;
                                }
                        }
                    }
                });
        }

        public DbColumns GetPrimaryKeyDbColumns()
        {
            if (mDbColumns is null)
                return null;
            var DbColumns = new DbColumns();
            foreach (DbColumn dbColumn in mDbColumns)
            {
                if (dbColumn.IsPrimaryKey)
                {
                    DbColumns.Add(dbColumn);
                }
            }
            return DbColumns;
        }

        public bool CheckBoundControlsForExistingPKey()
        {
            if (mIdentityDBColumn is object)
                return false;
            int i = 0;
            DbColumn dbcolumn;
            var PkDbColumns = new DbColumns();
            BoundControl BoundControl;
            //DataRow Row = null;
            object value = null;
            string SQLWhere = "";
            string SQL = "";
            string Parameter = "";
            DbCommand lobjcommand;
            lobjcommand = objFactory.CreateCommand();
            lobjcommand.Parameters.Clear();
            if (mDbObjectType != DbObjectTypeEnum.Table)
                return default;
            PkDbColumns = GetPrimaryKeyDbColumns();
            foreach (DbColumn currentDbcolumn in PkDbColumns)
            {
                dbcolumn = currentDbcolumn;
                foreach (BoundControl currentBoundControl in dbcolumn.BoundControls)
                {
                    BoundControl = currentBoundControl;
                    switch (BoundControl.BindingBehaviour)
                    {
                        case BasicDAL.BindingBehaviour.ReadWrite or BasicDAL.BindingBehaviour.ReadWriteAddNew:
                            {
                                value = Interaction.CallByName(BoundControl.Control, BoundControl.PropertyName, CallType.Get);
                                value = Utilities.DbValueCast(value, dbcolumn.DbType);
                                switch (dbcolumn.DbType)
                                {
                                    case DbType.AnsiString:
                                    case DbType.AnsiStringFixedLength:
                                    case DbType.String:
                                    case DbType.StringFixedLength:
                                        {
                                            //If(dbcolumn.OriginalValue.trim <> value.trim) Or(mAddNewStatus = True) Then
                                            //if (Conversions.ToBoolean(Operators.OrObject(Operators.ConditionalCompareObjectNotEqual(dbcolumn.OriginalValue.ToString ().Trim(), value.ToString ().Trim(), false), mAddNewStatus == true)))

                                            if (dbcolumn.OriginalValue.ToString().Trim() != value.ToString().Trim() || mAddNewStatus == true)
                                            {
                                                i = i + 1;
                                            }

                                            break;
                                        }

                                    default:
                                        {
                                            //if (Conversions.ToBoolean(Operators.OrObject(Operators.ConditionalCompareObjectNotEqual(dbcolumn.OriginalValue, value, false), mAddNewStatus == true)))
                                            if (dbcolumn.OriginalValue.ToString().Trim() != value.ToString().Trim() || mAddNewStatus == true)
                                            {
                                                i = i + 1;
                                            }

                                            break;
                                        }
                                }

                                break;
                            }
                    }
                }
            }

            if (i > 0)
            {
                foreach (DbColumn currentDbcolumn1 in PkDbColumns)
                {
                    dbcolumn = currentDbcolumn1;
                    foreach (BoundControl currentBoundControl1 in dbcolumn.BoundControls)
                    {
                        BoundControl = currentBoundControl1;
                        switch (BoundControl.BindingBehaviour)
                        {
                            case BasicDAL.BindingBehaviour.ReadWrite or BasicDAL.BindingBehaviour.ReadWriteAddNew:
                                {
                                    value = Interaction.CallByName(BoundControl.Control, BoundControl.PropertyName, CallType.Get);
                                    value = Utilities.DbValueCast(value, dbcolumn.DbType, NullDateValue, dbcolumn.DateTimeResolution);

                                    // Parameter = Me.DbParameterNamePrefix & "p" + Trim(i)
                                    Parameter = mDbConfig.ParameterNamePrefixE + i.ToString().Trim();

                                    // SQLWhere = SQLWhere & dbcolumn.DBColumnName & "=" & DbParameterNamePrefix & "p" & i & " AND "
                                    SQLWhere = SQLWhere + dbcolumn.DbColumnName + "=" + Parameter + " AND ";
                                    int parindex = lobjcommand.Parameters.Add(Parameter);
                                    lobjcommand.Parameters[parindex].Value = value;
                                    i = i + 1;
                                    break;
                                }
                        }
                    }
                }
            }

            if (string.IsNullOrEmpty(SQLWhere))
                return false;

            if (SQLWhere.Length > 5)
                SQLWhere = SQLWhere.Substring(0, SQLWhere.Length - 5);

            SQL = "SELECT COUNT(*) FROM " + DbTableName + " WHERE " + SQLWhere;
            lobjcommand.CommandText = SQL;
            lobjcommand.CommandType = CommandType.Text;
            lobjcommand.Connection = DbConnection; // objConnection
            object o = null;
            try
            {
                if (DbConnection.State == System.Data.ConnectionState.Closed)
                {
                    DbConnection.Open();
                }

                o = lobjcommand.ExecuteScalar();

            }
            catch (Exception ex)
            {
                HandleExceptions(ex);
            }
            finally
            {
                lobjcommand.Parameters.Clear();
                // objConnection.Close()

            }

            lobjcommand.Dispose();
            if (Convert.ToUInt32(o) != 0)
            {
                DuplicateKeyMessage(PkDbColumns);
                return true;
            }
            else
            {
                return false;
            }
        }

        public bool CheckBoundControlsForExistingFKeys(string WindowTitle, string Message)
        {
            object value;
            //int validated = 0;
            foreach (DbColumn DbColumn in mDbColumns)
            {
                if (DbColumn.DbLookup.DbObject is object)
                {
                    foreach (BoundControl Control in DbColumn.BoundControls)
                    {
                        if (Control.BindingBehaviour == BasicDAL.BindingBehaviour.ReadWrite | Control.BindingBehaviour == BasicDAL.BindingBehaviour.ReadWriteAddNew)
                        {
                            value = Interaction.CallByName(Control.Control, Control.PropertyName, CallType.Get);
                            value = Utilities.DbValueCast(value, DbColumn.DbType, NullDateValue, DbColumn.DateTimeResolution);
                            DbColumn.DbLookup.LookUp(value);
                            if (DbColumn.DbLookup.Validated == false)
                            {
                                if (SuppressErrorsNotification == false)
                                {
                                    Interaction.CallByName(this.mRedirectErrorsNotificationTo, "Show", CallType.Method, Message + "\r\n" + DbColumn.FriendlyName);
                                }

                                Control.Control.Focus();
                                return false;
                            }
                        }
                    }
                }
            }

            return true;
        }

        public object BuildSQLJoin()
        {
            DbJoinCondition JoinCondition = null;
            int i = 0;
            string sql = "";
            string sqljoin = "";
            string sqlcolumns = "";

            //Dim Join As Join
            //Dim Field As Field


            if (Fields == null)
            {
                Fields = GetFields(this);
            }

            if (Joins == null)
            {
                Joins = GetJoins(this);
            }

            foreach (Field Field in Fields)
            {
                if (!string.IsNullOrEmpty(Field.DbColumnNameAlias))
                {
                    //sqlcolumns = Operators.AddObject(Operators.AddObject(Operators.AddObject(Operators.AddObject(sqlcolumns, Field.DbColumnName), " AS "), Field.DbColumnNameAlias), ",");
                    sqlcolumns = sqlcolumns + Field.DbColumnName + " AS " + Field.DbColumnNameAlias + ",";
                }
                else
                {
                    //sqlcolumns = Operators.AddObject(Operators.AddObject(sqlcolumns, Field.DbColumnName), ",");
                    sqlcolumns = sqlcolumns + Field.DbColumnName + ",";
                }
            }

            if (sqlcolumns.Length > 1)
            {
                //sqlcolumns = Strings.Mid(Conversions.ToString(sqlcolumns), 1, Strings.Len(sqlcolumns) - 1);
                sqlcolumns = sqlcolumns.Substring(0, sqlcolumns.Length - 1);
            }

            string sSELECT = "SELECT ";

            if (mDistinctClause == true)
            {
                sSELECT = "SELECT DISTINCT ";
            }

            //sql = Operators.AddObject(Operators.AddObject(Operators.AddObject(Operators.AddObject(sSELECT, sqlcolumns), " FROM "), DbTableName), " ");
            sql = sSELECT + sqlcolumns + " FROM " + DbTableName + " ";

            foreach (Join Join in Joins)
            {
                if (Join.DbJoin != null)
                {
                    //sql = Operators.AddObject(sql, Join.DbJoin.SQLJoin());
                    sql = sql + Join.DbJoin.SQLJoin();
                }
            }

            return sql;
        }

        public object GetMaxDbValue(DbObject DbObject, DbColumn DbColumn)
        {
            string sql;
            object value;
            string sqlCount = "SELECT COUNT(" + DbColumn.DbColumnName + ") as ROWS FROM " + DbObject.DbTableName;
            sql = "SELECT MAX(" + DbColumn.DbColumnName + ") AS retunvalue from " + DbObject.DbTableName;
            value = DbObject.ExecuteScalar(sql, CommandType.Text, ConnectionState.KeepOpen);
            return Utilities.DbValueCast(value, DbColumn.DbType, NullDateValue, DbColumn.DateTimeResolution);
        }

        public object GetMaxDbValue(DbColumn DbColumn)
        {
            string sql;
            object value;
            sql = "SELECT MAX(" + DbColumn.DbColumnName + ") AS retunvalue from " + DbTableName;
            value = ExecuteScalar(sql, CommandType.Text, ConnectionState.KeepOpen);
            return Utilities.DbValueCast(value, DbColumn.DbType, NullDateValue, DbColumn.DateTimeResolution);
        }
        public DbObjectTypeEnum DbObjectType
        {
            get
            {
                DbObjectTypeEnum DbObjectTypeRet = default;
                DbObjectTypeRet = mDbObjectType;
                return DbObjectTypeRet;
            }

            set
            {
                mDbObjectType = value;
            }
        }

        [Obsolete ("Obsolete, use DbOrderBy properties.")]
        public string OrderBy
        {
            get
            {
                string OrderByRet = default;
                OrderByRet = mOrderBy;
                return OrderByRet;
            }

            set
            {
                mOrderBy = value;
            }
        }

        public DataBoundControlsBehaviour DataBinding
        {
            get
            {
                DataBoundControlsBehaviour DataBindingRet = default;
                DataBindingRet = mDataBinding;
                return DataBindingRet;
            }

            set
            {
                mDataBinding = value;
            }
        }

        public bool AddNewStatus
        {
            get
            {
                bool AddNewStatusRet = default;
                AddNewStatusRet = mAddNewStatus;
                return AddNewStatusRet;
            }

            set
            {
                mAddNewStatus = value;
            }
        }

        public dynamic CurrencyManager
        {
            get
            {
                dynamic CurrencyManagerRet = default;
                CurrencyManagerRet = mCurrencyManager;
                return CurrencyManagerRet;
            }

            set
            {
                mCurrencyManager = value;
            }
        }

        public InterfaceModeEnum InterfaceMode
        {
            get
            {
                InterfaceModeEnum InterfaceModeRet = default;
                InterfaceModeRet = mInterfaceMode;
                return InterfaceModeRet;
            }

            set
            {
                mInterfaceMode = value;
            }
        }

        public void DataBindingClear()
        {
            ClearAllBoundControls();
        }

        public bool DbObjectInitialized
        {
            get
            {
                return mDbObjectInitialized;
            }
        }

        public bool DbObjectIsValid
        {
            get
            {
                if (objDataTable == null == false)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        public void UndoChanges()
        {
            if (mCurrentPosition < 0L)
                return;
            bool Cancel = false;
            if (DisableEvents == false)
            {
                DataEventBefore?.Invoke(DataEventType.UndoChanges, ref Cancel);
            }

            if (Cancel == true)
                return;
            if (mAddNewStatus == true)
            {
                try
                {
                    objDataTable.Rows[(int)mCurrentPosition].Delete();
                    objDataTable.AcceptChanges();
                    mAddNewStatus = false;
                }
                catch
                {
                }
            }

            if (mDataBinding == DataBoundControlsBehaviour.WindowsFormsDataBinding)
            {
                CurrencyManager.CancelCurrentEdit();
                CurrencyManager.Refresh();
            }

            if (mCurrentPosition < 0)
            {
                DataRowGetValues(null, mDataBinding);
            }
            else if (mCurrentPosition > -1)
            {
                try
                {
                    DataRowGetValues(objDataTable.Rows[(int)mCurrentPosition], mDataBinding);
                }
                catch
                {
                    try
                    {
                        DataRowGetValues(null, mDataBinding);
                    }
                    catch
                    {
                    }
                }
            }

            mRowCount = objDataTable.Rows.Count;
            mAddNewStatus = false;
            if (DisableEvents == false)
            {
                DataEventAfter?.Invoke(DataEventType.UndoChanges);
            }
        }

        public System.Data.Common.DbParameter CreateDbParameter(string ParameterName, DbType DbType, ParameterDirection Direction, int Size = 0)
        {
            if (objFactory is null)
                return null;
            var parameter = objFactory.CreateParameter();

            parameter.ParameterName = ParameterName;
            parameter.DbType = DbType;
            parameter.Direction = Direction;
            parameter.Size = Size;


            // Select Case DbType
            // Case Is = Data.DbType.AnsiString, Data.DbType.AnsiStringFixedLength, Data.DbType.String, Data.DbType.StringFixedLength

            // Case Is = Data.DbType.Byte

            // Case Is = Data.DbType.Boolean

            // Case Is = Data.DbType.Currency, Data.DbType.Decimal, Data.DbType.VarNumeric

            // Case Is = DbType.Single

            // Case Is = Data.DbType.Date, Data.DbType.DateTime

            // Case Is = Data.DbType.Time

            // Case Is = Data.DbType.Double

            // Case Is = Data.DbType.Guid

            // Case Is = Data.DbType.SByte

            // Case Is = Data.DbType.Int16

            // Case Is = Data.DbType.Int32

            // Case Is = Data.DbType.Int64

            // Case Is = Data.DbType.UInt16

            // Case Is = Data.DbType.UInt32

            // Case Is = Data.DbType.UInt64

            // Case Else

            // End Select
            if (DbType ==  DbType.Time )
            {
                int zzz = 0;
            }

            
            return parameter;
        }

        public void UnloadAll()
        {
            try
            {
                objDataTable.Clear();
                objDataTable.Dispose();
                mRowCount = 0;
                mCurrentPosition = -1;
                CurrentDataRow = null;
                DataRowGetValues(null, mDataBinding);
            }
            catch
            {
            }
        }

        public void Open(bool LoadAllRows = false)
        {
            DoQuery(LoadAllRows);
        }

        public void DoQuery(bool LoadAllRows = true)
        {
            var f = new DbFilters();
            DbColumns dbpks;
            if (LoadAllRows == false)
            {
                dbpks = GetPrimaryKeyDbColumns();
                foreach (DbColumn dbpk in dbpks)
                {

                    switch (mDbConfig.Provider)
                    {
                        case Providers.OracleDataAccess:
#if COMPILEORACLE
                            f.Add(dbpk, ComparisionOperator.IsNull, DBNull.Value, LogicOperator.AND);
#endif
                            break;

                        default:
                            {
                                f.Add(dbpk, ComparisionOperator.Equals, DBNull.Value, LogicOperator.AND);
                                break;
                            }
                    }
                }

                if (dbpks.Count > 0)
                {
                    f.get_Item(f.Count - 1).LogicOperator = LogicOperator.None;
                    FiltersGroup.Clear();
                    FiltersGroup.Add(f);
                }
            }

            DoQuery();
            if (LoadAllRows == false)
            {
                FiltersGroup.Clear();
            }
        }

        [Obsolete ("Use DoQuery()")]
        public void LoadAll()
        {
            DoQuery();
        }
        public void ExecuteProcedure()
        {
            DoQuery();
        }

        public void ExecuteFunction()
        {
            DoQuery();
        }

        public void DoQuery()
        {
            //int iColumn = 0;
            bool Cancel = false;
            ResetError();
            mAddNewStatus = false;
            mRowCount = 0;
            if (mDataBinding != DataBoundControlsBehaviour.NoDataBinding)
            {
                InitBoundControls();
            }

            if (DisableEvents == false)
            {
                DataEventBefore?.Invoke(DataEventType.Query, ref Cancel);
            }

            if (Cancel == true)
                return;


            
            switch (UseDataReader)
            {
                case false:
                    {
                        if (DbObjectType == DbObjectTypeEnum.InMemoryJoin)
                        {
                            BuildInMemoryJoinDataTable();
                        }
                        else
                        {
                            try
                            {
                                switch (mDbObjectType)
                                {
                                    case DbObjectTypeEnum.StoredProcedure:
                                        {
                                            objDataSet = ExecuteStoredProcedure();
                                            break;
                                        }

                                    case DbObjectTypeEnum.TableFunction:
                                        {
                                            objDataSet = ExecuteTableScalarFunction();
                                            break;
                                        }

                                    case DbObjectTypeEnum.ScalarFunction:
                                        {
                                            objDataSet = ExecuteTableScalarFunction();
                                            break;
                                        }

                                    default:
                                        {
                                            objDataSet = ExecuteDataSet();
                                            break;
                                        }
                                }

                                if (objDataSet.Tables.Count > 0)
                                {
                                    objDataTable = objDataSet.Tables[0];
                                    mRowCount = objDataSet.Tables[0].Rows.Count;
                                    MoveFirst();
                                }
                            }
                            catch (Exception ex)
                            {
                                // If (mRowCount) Then
                                this.HandleExceptions(ex, "LoadAll");
                                // End If

                            }
                        }

                        break;
                    }

                case true:
                    {
                        try
                        {
                            GetEmptyDataTable();
                            mRowCount = 0;
                            switch (mDbObjectType)
                            {
                                case DbObjectTypeEnum.StoredProcedure:
                                    {
                                        break;
                                    }

                                default:
                                    {
                                        objDataReader = ExecuteReader();
                                        break;
                                    }
                            }

                            objDataReader.Read();
                            if (objDataReader.HasRows == true)
                            {
                                MoveFirst();
                                mRowCount = 1;
                            }
                            else
                            {
                                mEOF = true;
                                mRowCount = 0;
                            }
                        }
                        catch
                        {
                        }

                        break;
                    }
            }

            mDataChanged = false;
            if (DisableEvents == false)
            {
                DataEventAfter?.Invoke(DataEventType.Query);
            }
        }

        private void DataRowGetValuesFromDataReader()
        {
            try
            {
                if (objDataTable.Rows.Count != 1)
                    return;
                if (objDataReader.IsClosed == true)
                    return;
                int iColumn = 0;

                string cName = "";
                int loopTo = objDataReader.FieldCount - 1;
                for (iColumn = 0; iColumn <= loopTo; iColumn++)
                {
                    cName = objDataReader.GetName(iColumn);
                    objDataTable.Rows[0][cName] = objDataReader[cName];
                }
            }
            catch (Exception ex)
            {
                HandleExceptions(ex, "DbObject.DataRowGetValuesFromDataReader()");
            }
        }

        public void GetEmptyDataTable()
        {
            DataSet ds;
            string CommandText;
            string WhereConditions;
            ResetError();
            CommandText = "SELECT * FROM " + DbTableName;
            WhereConditions = " 0<>0 ";
            CommandText = CommandText + " WHERE " + WhereConditions;
            ds = ExecuteDataSet(CommandText);
            mRowCount = 0;
            objDataTable = ds.Tables[0];
            objDataTable.Rows.Add(null, null);
        }

        public void GetEmptyDataTable(ref DataTable dt )
        {
            DataSet ds;
            string CommandText;
            string WhereConditions;
            ResetError();
            CommandText = "SELECT * FROM " + DbTableName;
            WhereConditions = " 0<>0 ";
            CommandText = CommandText + " WHERE " + WhereConditions;
            ds = ExecuteDataSet(CommandText);
            mRowCount = 0;
            dt = ds.Tables[0];
            dt.Rows.Add(null, null);
        }

        public string GetSQLSelectCommandTextForObjectSchema()
        {
            string CommandText = "";
            switch (DbObjectType)
            {
                case DbObjectTypeEnum.Table or DbObjectTypeEnum.View:
                    {
                        switch (mDbConfig.Provider)
                        {
                            case Providers.SqlServer:
                                {
                                    CommandText = "SELECT TOP(0) * FROM " + DbTableName;
                                    break;
                                }

                            case Providers.OracleDataAccess:
                                {
#if COMPILEORACLE
                                    CommandText = "SELECT  * FROM " + DbTableName + " WHERE ROWNUM < 1";
#endif
                                    break;
                                }
                            case Providers.ODBC_DB2 or Providers.OleDb_DB2 or Providers.DB2_iSeries:
                                {
                                    CommandText = "SELECT * FROM " + DbTableName + " FETCH FIRST 1 ROWS ONLY ";
                                    break;
                                }

                            case  Providers.MySQL :
                                {
                                    CommandText = "SELECT * FROM "+ DbTableName + " LIMIT 1";
                                    break;
                                }
                            default:
                                {
                                    CommandText = "SELECT * FROM " + DbTableName + " WHERE 0 <>0";
                                    break;
                                }
                        }

                        break;
                    }

                case DbObjectTypeEnum.SQLQuery:
                    {
                        CommandText = SQLQuery;
                        break;
                    }

                default:
                    {
                        break;
                    }
            }

            return CommandText;
        }

        public void GetDbSchema(ConnectionState connectionstate)
        {
            DbDataReader dr = null;
            DataTable dt = null;
            //int i = 0;
            DataRow r;
            DataView dv;
            string CommandText = "";
            ResetError();
            switch (DbObjectType)
            {
                case DbObjectTypeEnum.Table or DbObjectTypeEnum.View:
                    {
                        switch (mDbConfig.Provider)
                        {
                            case Providers.SqlServer:
                                {
                                    CommandText = "SELECT TOP(0) * FROM " + DbTableName;
                                    break;
                                }

                            case Providers.OracleDataAccess:

                                CommandText = "SELECT  * FROM " + DbTableName + " WHERE ROWNUM < 1";
                                break;


                            case Providers.ODBC_DB2:
                                {
                                    CommandText = "SELECT * FROM " + DbTableName + " FETCH FIRST 1 ROWS ONLY ";
                                    break;
                                }

                            case Providers.OleDb_DB2:
                                {
                                    CommandText = "SELECT * FROM " + DbTableName + " FETCH FIRST 1 ROWS ONLY";
                                    break;
                                }

                            default:
                                {
                                    CommandText = "SELECT * FROM " + DbTableName + " WHERE 0<>0";
                                    break;
                                }
                        }

                        break;
                    }

                case DbObjectTypeEnum.SQLQuery:
                    {
                        CommandText = SQLQuery;
                        break;
                    }

                default:
                    {
                        return;
                    }
            }

            bool UseCachedConnection = false;
            DbConnection lobjConnection;
            DbCommand lobjCommand;
            lobjConnection = objFactory.CreateConnection();
            lobjCommand = objFactory.CreateCommand();
            if (mCachedConnection == null == true)
            {
                lobjConnection.ConnectionString = mConnectionString;
            }
            else
            {
                UseCachedConnection = true;
                lobjConnection = mCachedConnection;
            }

            try
            {
                lobjCommand.Connection = lobjConnection;
                lobjCommand.CommandType = CommandType.Text;
                lobjCommand.CommandText = CommandText;
                if (string.IsNullOrEmpty(lobjCommand.Connection.ConnectionString))
                {
                    lobjCommand.Connection.ConnectionString = DbConfig.ConnectionString;
                }

                if (lobjCommand.Connection.State == System.Data.ConnectionState.Closed)
                {
                    lobjCommand.Connection.Open();
                }

                dr = lobjCommand.ExecuteReader();
                dt = dr.GetSchemaTable();
                dr.Close();
                lobjCommand.Dispose();
                switch (UseCachedConnection)
                {
                    case true:
                        {
                            break;
                        }

                    default:
                        {
                            lobjConnection.Close();
                            lobjConnection.Dispose();
                            break;
                        }
                }
            }
            catch (Exception ex)
            {
                lobjCommand.Dispose();
                switch (UseCachedConnection)
                {
                    case true:
                        {
                            break;
                        }

                    default:
                        {
                            lobjConnection.Close();
                            lobjConnection.Dispose();
                            break;
                        }
                }

                HandleExceptions(ex, "DbObject.GetDbSchema()");
                mLastErrorCode = 1;
                lobjConnection.Close();
                lobjConnection.Dispose();
                return;
            }

            foreach (DbColumn dbcolumn in mDbColumns)
            {
                switch (mDbConfig.Provider)
                {
                    case Providers.OracleDataAccess:

                        {
                            dv = new DataView(dt, "ColumnName='" + dbcolumn.DbColumnNameE.Replace("\"", "") + "'", "", DataViewRowState.CurrentRows);
                            break;
                        }
                    case Providers.OleDb_DB2:
                        {
                            dv = new DataView(dt, "ColumnName='" + dbcolumn.DbColumnNameE + "'", "", DataViewRowState.CurrentRows);
                            break;
                        }

                    default:
                        {
                            dv = new DataView(dt, "ColumnName='" + dbcolumn.DbColumnNameE + "'", "", DataViewRowState.CurrentRows);
                            break;
                        }
                }

                if (dv.Count == 1)
                {
                    object argReturnValue = null;
                    r = dv[0].Row;
                    switch (mDbConfig.Provider)
                    {
                        case Providers.SqlServer:
                            {

                                // ColumnName(-0) - NOT ASSIGNED
                                // ColumnOrdinal(-1)
                                // ColumnSize(-2)
                                // NumericPrecision(-3)
                                // NumericScale(-4)
                                // IsUnique(-5)
                                // IsKey(-6)
                                // BaseServerName(-7)
                                // BaseCatalogName(-8)
                                // BaseColumnName(-9)
                                // BaseSchemaName(-10)
                                // BaseTableName(-11)
                                // DataType(-12)
                                // AllowDBNull(-13)
                                // ProviderType(-14)
                                // IsAliased(-15)
                                // IsExpression(-16)
                                // IsIdentity(-17)
                                // IsAutoIncrement(-18)
                                // IsRowVersion(-19)
                                // IsHidden(-20)
                                // IsLong(-21)
                                // IsReadOnly(-22)
                                // ProviderSpecificDataType(-23)
                                // DataTypeName(-24)
                                // XmlSchemaCollectionDatabase(-25) -- NOT ASSIGNED
                                // XmlSchemaCollectionOwningSchema(-26) -- NOT ASSIGNED
                                // XmlSchemaCollectionName(-27) -- NOT ASSIGNED
                                // UdtAssemblyQualifiedName(-28)
                                // NonVersionedProviderType(-29) -- NOT ASSIGNED
                                // IsColumnSet(-30) -- NOT ASSIGNED



                                argReturnValue = dbcolumn.BaseServerName;
                                AssignValue(ref argReturnValue, r["BaseServerName"]);
                                dbcolumn.BaseServerName = Convert.ToString(argReturnValue);
                                argReturnValue = dbcolumn.BaseServerName;
                                AssignValue(ref argReturnValue, r["DataTypeName"]);
                                dbcolumn.DataTypeName = Convert.ToString(argReturnValue);
                                argReturnValue = dbcolumn.IsAliased;
                                AssignValue(ref argReturnValue, r["IsAliased"]);
                                dbcolumn.IsAliased = Convert.ToBoolean(argReturnValue);
                                argReturnValue = dbcolumn.IsIdentity;
                                AssignValue(ref argReturnValue, r["IsIdentity"]);
                                dbcolumn.IsIdentity = Convert.ToBoolean(argReturnValue);
                                argReturnValue = dbcolumn.IsHidden;
                                AssignValue(ref argReturnValue, r["IsHidden"]);
                                dbcolumn.IsHidden = Convert.ToBoolean(argReturnValue);
                                argReturnValue = dbcolumn.udtAssemblyQualifiedName;
                                AssignValue(ref argReturnValue, r["UdtAssemblyQualifiedName"]);
                                dbcolumn.udtAssemblyQualifiedName = Convert.ToString(argReturnValue);
                                argReturnValue = dbcolumn.IsExpression;
                                AssignValue(ref argReturnValue, r["IsExpression"]);
                                dbcolumn.IsExpression = Convert.ToBoolean(argReturnValue);
                                argReturnValue = dbcolumn.ProviderSpecificDataType;
                                AssignValue(ref argReturnValue, r["ProviderSpecificDataType"]);
                                dbcolumn.ProviderSpecificDataType = (Type)argReturnValue;
                                argReturnValue = dbcolumn.AllowDBNull;
                                AssignValue(ref argReturnValue, r["AllowDBNull"]);
                                dbcolumn.AllowDBNull = Convert.ToBoolean(argReturnValue);
                                argReturnValue = dbcolumn.AllowNull;
                                AssignValue(ref argReturnValue, dbcolumn.AllowDBNull);
                                dbcolumn.AllowNull = Convert.ToBoolean(argReturnValue);
                                argReturnValue = dbcolumn.BaseCatalogName;
                                AssignValue(ref argReturnValue, r["BaseCatalogName"]);
                                dbcolumn.BaseCatalogName = Convert.ToString(argReturnValue);
                                argReturnValue = dbcolumn.BaseColumnName;
                                AssignValue(ref argReturnValue, r["BaseColumnName"]);
                                dbcolumn.BaseColumnName = Convert.ToString(argReturnValue);
                                argReturnValue = dbcolumn.BaseTableName;
                                AssignValue(ref argReturnValue, r["BaseTableName"]);
                                dbcolumn.BaseTableName = Convert.ToString(argReturnValue);
                                argReturnValue = dbcolumn.BaseSchemaName;
                                AssignValue(ref argReturnValue, r["BaseSchemaName"]);
                                dbcolumn.BaseSchemaName = Convert.ToString(argReturnValue);
                                argReturnValue = dbcolumn.DataType;
                                AssignValue(ref argReturnValue, r["DataType"]);
                                dbcolumn.DataType = (Type)argReturnValue;
                                argReturnValue = dbcolumn.ColumnOrdinal;
                                AssignValue(ref argReturnValue, r["ColumnOrdinal"]);
                                dbcolumn.ColumnOrdinal = Convert.ToInt32(argReturnValue);
                                argReturnValue = dbcolumn.Size;
                                AssignValue(ref argReturnValue, r["ColumnSize"]);
                                dbcolumn.Size = Convert.ToInt32(argReturnValue);
                                argReturnValue = dbcolumn.NumericPrecision;
                                AssignValue(ref argReturnValue, r["NumericPrecision"]);
                                dbcolumn.NumericPrecision = Convert.ToInt32(argReturnValue);
                                argReturnValue = dbcolumn.NumericScale;
                                AssignValue(ref argReturnValue, r["NumericScale"]);
                                dbcolumn.NumericScale = Convert.ToInt32(argReturnValue);
                                argReturnValue = dbcolumn.IsUnique;
                                AssignValue(ref argReturnValue, r["IsUnique"]);
                                dbcolumn.IsUnique = Convert.ToBoolean(argReturnValue);
                                argReturnValue = dbcolumn.IsKey;
                                AssignValue(ref argReturnValue, r["IsKey"]);
                                dbcolumn.IsKey = Convert.ToBoolean(argReturnValue);
                                argReturnValue = dbcolumn.ProviderType;
                                AssignValue(ref argReturnValue, r["ProviderType"]);
                                dbcolumn.ProviderType = Convert.ToInt32(argReturnValue);
                                argReturnValue = dbcolumn.IsAutoIncrement;
                                AssignValue(ref argReturnValue, r["IsAutoIncrement"]);
                                dbcolumn.IsAutoIncrement = Convert.ToBoolean(argReturnValue);
                                //argReturnValue = dbcolumn.AutoIncrementStep;
                                //AssignValue(ref argReturnValue , r["IsAutoIncrement.AutoIncrementStep"]);
                                //dbcolumn.AutoIncrementStep = argReturnValue;
                                //argReturnValue = dbcolumn.AutoIncrementSeed;
                                //AssignValue(ref argReturnValue, r["IsAutoIncrement.AutoIncrementSeed"]);
                                //dbcolumn.AutoIncrementSeed = argReturnValue;
                                argReturnValue = dbcolumn.IsRowVersion;
                                AssignValue(ref argReturnValue, r["IsRowVersion"]);
                                dbcolumn.IsRowVersion = Convert.ToBoolean(argReturnValue);
                                argReturnValue = dbcolumn.IsLong;
                                AssignValue(ref argReturnValue, r["IsLong"]);
                                dbcolumn.IsLong = Convert.ToBoolean(argReturnValue);
                                argReturnValue = dbcolumn.IsReadOnly;
                                AssignValue(ref argReturnValue, r["IsReadOnly"]);
                                dbcolumn.IsReadOnly = Convert.ToBoolean(argReturnValue);

                                // modifica per Identity SQLServer

                                if (dbcolumn.IsIdentity == true)
                                {
                                    mIdentityDBColumn = dbcolumn;

                                }

                                break;
                            }
                        case Providers.OracleDataAccess:
#if COMPILEORACLE
                            //ColumnName(-0) ' TO UPPERCASE
                            //argReturnValue = dbcolumn.DbColumnName ;
                            //AssignValue(ref argReturnValue, r["ColumnName"]);
                            //dbcolumn.DbColumnName = Convert.ToString(argReturnValue).ToUpper();
                            //ColumnOrdinal(-1)
                            argReturnValue = dbcolumn.ColumnOrdinal;
                            AssignValue(ref argReturnValue, r["ColumnOrdinal"]);
                            dbcolumn.ColumnOrdinal = Convert.ToInt32(argReturnValue);
                            //ColumnSize(-2)
                            argReturnValue = dbcolumn.Size;
                            AssignValue(ref argReturnValue, r["ColumnSize"]);
                            dbcolumn.Size = Convert.ToInt32(argReturnValue);
                            //NumericPrecision(-3)
                            argReturnValue = dbcolumn.NumericPrecision;
                            AssignValue(ref argReturnValue, r["NumericPrecision"]);
                            dbcolumn.NumericPrecision = Convert.ToInt32(argReturnValue);
                            //NumericScale(-4)
                            argReturnValue = dbcolumn.NumericScale;
                            AssignValue(ref argReturnValue, r["NumericScale"]);
                            dbcolumn.NumericScale = Convert.ToInt32(argReturnValue);
                            //IsUnique(-5)
                            argReturnValue = dbcolumn.IsUnique;
                            AssignValue(ref argReturnValue, r["IsUnique"]);
                            dbcolumn.IsUnique = Convert.ToBoolean(argReturnValue);
                            //IsKey(-6)
                            argReturnValue = dbcolumn.IsKey;
                            AssignValue(ref argReturnValue, r["IsKey"]);
                            dbcolumn.IsKey = Convert.ToBoolean(argReturnValue);
                            //IsRowID(-7)
                            argReturnValue = dbcolumn.IsRowID;
                            AssignValue(ref argReturnValue, r["IsRowID"]);
                            dbcolumn.IsRowID = Convert.ToBoolean(argReturnValue);
                            //BaseColumnName(-8)
                            argReturnValue = dbcolumn.BaseColumnName;
                            AssignValue(ref argReturnValue, r["BaseColumnName"]);
                            dbcolumn.BaseColumnName = Convert.ToString(argReturnValue);
                            //BaseSchemaName(-9)
                            argReturnValue = dbcolumn.BaseSchemaName;
                            AssignValue(ref argReturnValue, r["BaseSchemaName"]);
                            dbcolumn.BaseSchemaName = Convert.ToString(argReturnValue);
                            //BaseTableName(-10)
                            argReturnValue = dbcolumn.BaseTableName;
                            AssignValue(ref argReturnValue, r["BaseTableName"]);
                            dbcolumn.BaseTableName = Convert.ToString(argReturnValue);
                            //DataType(-11)
                            argReturnValue = dbcolumn.DataType;
                            AssignValue(ref argReturnValue, r["DataType"]);
                            dbcolumn.DataType = (Type)argReturnValue;
                            //ProviderType(-12)
                            argReturnValue = dbcolumn.ProviderType;
                            AssignValue(ref argReturnValue, r["ProviderType"]);
                            dbcolumn.ProviderType = Convert.ToInt32(argReturnValue);
                            //AllowDBNull(-13)
                            argReturnValue = dbcolumn.AllowDBNull;
                            AssignValue(ref argReturnValue, r["AllowDBNull"]);
                            dbcolumn.AllowDBNull = Convert.ToBoolean(argReturnValue);
                            //IsAliased(-14)
                            argReturnValue = dbcolumn.IsAliased;
                            AssignValue(ref argReturnValue, r["IsAliased"]);
                            dbcolumn.IsAliased = Convert.ToBoolean(argReturnValue);
                            //IsByteSemantic(-15)
                            argReturnValue = dbcolumn.IsByteSemantic;
                            AssignValue(ref argReturnValue, r["IsByteSemantic"]);
                            dbcolumn.IsByteSemantic = Convert.ToBoolean(argReturnValue);
                            //IsExpression(-16)
                            argReturnValue = dbcolumn.IsExpression;
                            AssignValue(ref argReturnValue, r["IsExpression"]);
                            dbcolumn.IsExpression = Convert.ToBoolean(argReturnValue);
                            //IsHidden(-17)
                            argReturnValue = dbcolumn.IsHidden;
                            AssignValue(ref argReturnValue, r["IsHidden"]);
                            dbcolumn.IsHidden = Convert.ToBoolean(argReturnValue);
                            //IsReadOnly(-18)
                            argReturnValue = dbcolumn.IsReadOnly;
                            AssignValue(ref argReturnValue, r["IsReadOnly"]);
                            dbcolumn.IsReadOnly = Convert.ToBoolean(argReturnValue);
                            //IsLong(-19)
                            argReturnValue = dbcolumn.IsLong;
                            AssignValue(ref argReturnValue, r["IsLong"]);
                            dbcolumn.IsLong = Convert.ToBoolean(argReturnValue);
                            //UdtTypeName(-20)
                            argReturnValue = dbcolumn.udtAssemblyQualifiedName;
                            AssignValue(ref argReturnValue, r["UdtTypeName"]);
                            //IsAutoIncrement(-21)
                            argReturnValue = dbcolumn.IsAutoIncrement;
                            AssignValue(ref argReturnValue, r["IsAutoIncrement"]);
                            dbcolumn.IsAutoIncrement = Convert.ToBoolean(argReturnValue);
                            //IsIdentity(-22)
                            argReturnValue = dbcolumn.IsIdentity;
                            AssignValue(ref argReturnValue, r["IsIdentity"]);
                            dbcolumn.IsIdentity = Convert.ToBoolean(argReturnValue);
                            //IdentityType(-23)
#endif 
                            break;
                        case Providers.MySQL:
                            argReturnValue = dbcolumn.AllowDBNull;
                            AssignValue(ref argReturnValue, r["AllowDBNull"]);
                            dbcolumn.AllowDBNull = Convert.ToBoolean(argReturnValue);
                            argReturnValue = dbcolumn.AllowNull;
                            AssignValue(ref argReturnValue, dbcolumn.AllowDBNull);
                            dbcolumn.AllowNull = Convert.ToBoolean(argReturnValue);
                            argReturnValue = dbcolumn.BaseCatalogName;
                            AssignValue(ref argReturnValue, r["BaseCatalogName"]);
                            dbcolumn.BaseCatalogName = Convert.ToString(argReturnValue);
                            argReturnValue = dbcolumn.BaseColumnName;
                            AssignValue(ref argReturnValue, r["BaseColumnName"]);
                            dbcolumn.BaseColumnName = Convert.ToString(argReturnValue);
                            argReturnValue = dbcolumn.BaseTableName;
                            AssignValue(ref argReturnValue, r["BaseTableName"]);
                            dbcolumn.BaseTableName = Convert.ToString(argReturnValue);
                            argReturnValue = dbcolumn.BaseSchemaName;
                            AssignValue(ref argReturnValue, r["BaseSchemaName"]);
                            dbcolumn.BaseSchemaName = Convert.ToString(argReturnValue);
                            argReturnValue = dbcolumn.DataType;
                            AssignValue(ref argReturnValue, r["DataType"]);
                            dbcolumn.DataType = (Type)argReturnValue;
                            argReturnValue = dbcolumn.ColumnOrdinal;
                            AssignValue(ref argReturnValue, r["ColumnOrdinal"]);
                            dbcolumn.ColumnOrdinal = Convert.ToInt32(argReturnValue);
                            argReturnValue = dbcolumn.Size;
                            AssignValue(ref argReturnValue, r["ColumnSize"]);
                            dbcolumn.Size = Convert.ToInt32(argReturnValue);
                            argReturnValue = dbcolumn.NumericPrecision;
                            AssignValue(ref argReturnValue, r["NumericPrecision"]);
                            dbcolumn.NumericPrecision = Convert.ToInt32(argReturnValue);
                            argReturnValue = dbcolumn.NumericScale;
                            AssignValue(ref argReturnValue, r["NumericScale"]);
                            dbcolumn.NumericScale = Convert.ToInt32(argReturnValue);
                            argReturnValue = dbcolumn.IsUnique;
                            AssignValue(ref argReturnValue, r["IsUnique"]);
                            dbcolumn.IsUnique = Convert.ToBoolean(argReturnValue);
                            argReturnValue = dbcolumn.IsKey;
                            AssignValue(ref argReturnValue, r["IsKey"]);
                            dbcolumn.IsKey = Convert.ToBoolean(argReturnValue);
                            argReturnValue = dbcolumn.ProviderType;
                            AssignValue(ref argReturnValue, r["ProviderType"]);
                            dbcolumn.ProviderType = Convert.ToInt32(argReturnValue);
                            argReturnValue = dbcolumn.IsAutoIncrement;
                            AssignValue(ref argReturnValue, r["IsAutoIncrement"]);
                            dbcolumn.IsAutoIncrement = Convert.ToBoolean(argReturnValue);
                            argReturnValue = dbcolumn.IsRowVersion;
                            AssignValue(ref argReturnValue, r["IsRowVersion"]);
                            dbcolumn.IsRowVersion = Convert.ToBoolean(argReturnValue);
                            argReturnValue = dbcolumn.IsLong;
                            AssignValue(ref argReturnValue, r["IsLong"]);
                            dbcolumn.IsLong = Convert.ToBoolean(argReturnValue);
                            argReturnValue = dbcolumn.IsReadOnly;
                            AssignValue(ref argReturnValue, r["IsReadOnly"]);
                            dbcolumn.IsReadOnly = Convert.ToBoolean(argReturnValue);
                            argReturnValue = dbcolumn.IsAliased;
                            AssignValue(ref argReturnValue, r["IsAliased"]);
                            dbcolumn.IsAliased = Convert.ToBoolean(argReturnValue);
                            argReturnValue = dbcolumn.IsExpression;
                            AssignValue(ref argReturnValue, r["IsExpression"]);
                            dbcolumn.IsExpression = Convert.ToBoolean(argReturnValue);
                            argReturnValue = dbcolumn.IsIdentity ;
                            AssignValue(ref argReturnValue, r["IsIdentity"]);
                            dbcolumn.IsIdentity = Convert.ToBoolean(argReturnValue);
                            argReturnValue = dbcolumn.IsHidden ;
                            AssignValue(ref argReturnValue, r["IsHidden"]);
                            dbcolumn.IsHidden = Convert.ToBoolean(argReturnValue);

                            break;

                        case Providers.DB2_iSeries :

                            argReturnValue = dbcolumn.AllowDBNull;
                            AssignValue(ref argReturnValue, r["AllowDBNull"]);
                            dbcolumn.AllowDBNull = Convert.ToBoolean(argReturnValue);
                            argReturnValue = dbcolumn.AllowNull;
                            AssignValue(ref argReturnValue, dbcolumn.AllowDBNull);
                            dbcolumn.AllowNull = Convert.ToBoolean(argReturnValue);
                            argReturnValue = dbcolumn.BaseCatalogName;
                            AssignValue(ref argReturnValue, r["BaseCatalogName"]);
                            dbcolumn.BaseCatalogName = Convert.ToString(argReturnValue);
                            argReturnValue = dbcolumn.BaseColumnName;
                            AssignValue(ref argReturnValue, r["BaseColumnName"]);
                            dbcolumn.BaseColumnName = Convert.ToString(argReturnValue);
                            argReturnValue = dbcolumn.BaseTableName;
                            AssignValue(ref argReturnValue, r["BaseTableName"]);
                            dbcolumn.BaseTableName = Convert.ToString(argReturnValue);
                            argReturnValue = dbcolumn.BaseSchemaName;
                            AssignValue(ref argReturnValue, r["BaseSchemaName"]);
                            dbcolumn.BaseSchemaName = Convert.ToString(argReturnValue);
                            argReturnValue = dbcolumn.DataType;
                            AssignValue(ref argReturnValue, r["DataType"]);
                            dbcolumn.DataType = (Type)argReturnValue;
                            argReturnValue = dbcolumn.ColumnOrdinal;
                            AssignValue(ref argReturnValue, r["ColumnOrdinal"]);
                            dbcolumn.ColumnOrdinal = Convert.ToInt32(argReturnValue);
                            argReturnValue = dbcolumn.Size;
                            AssignValue(ref argReturnValue, r["ColumnSize"]);
                            dbcolumn.Size = Convert.ToInt32(argReturnValue);
                            argReturnValue = dbcolumn.NumericPrecision;
                            AssignValue(ref argReturnValue, r["NumericPrecision"]);
                            dbcolumn.NumericPrecision = Convert.ToInt32(argReturnValue);
                            argReturnValue = dbcolumn.NumericScale;
                            AssignValue(ref argReturnValue, r["NumericScale"]);
                            dbcolumn.NumericScale = Convert.ToInt32(argReturnValue);
                            argReturnValue = dbcolumn.IsUnique;
                            AssignValue(ref argReturnValue, r["IsUnique"]);
                            dbcolumn.IsUnique = Convert.ToBoolean(argReturnValue);
                            argReturnValue = dbcolumn.IsKey;
                            AssignValue(ref argReturnValue, r["IsKey"]);
                            dbcolumn.IsKey = Convert.ToBoolean(argReturnValue);
                            argReturnValue = dbcolumn.ProviderType;
                            AssignValue(ref argReturnValue, r["ProviderType"]);
                            dbcolumn.ProviderType = Convert.ToInt32(argReturnValue);
                            argReturnValue = dbcolumn.IsAutoIncrement;
                            AssignValue(ref argReturnValue, r["IsAutoIncrement"]);
                            dbcolumn.IsAutoIncrement = Convert.ToBoolean(argReturnValue);
                            argReturnValue = dbcolumn.IsRowVersion;
                            AssignValue(ref argReturnValue, r["IsRowVersion"]);
                            dbcolumn.IsRowVersion = Convert.ToBoolean(argReturnValue);
                            argReturnValue = dbcolumn.IsLong;
                            AssignValue(ref argReturnValue, r["IsLong"]);
                            dbcolumn.IsLong = Convert.ToBoolean(argReturnValue);
                            argReturnValue = dbcolumn.IsReadOnly;
                            AssignValue(ref argReturnValue, r["IsReadOnly"]);
                            dbcolumn.IsReadOnly = Convert.ToBoolean(argReturnValue);
                            argReturnValue = dbcolumn.IsIdentity;
                            AssignValue(ref argReturnValue, r["IsIdentity"]);
                            dbcolumn.IsIdentity = Convert.ToBoolean(argReturnValue);
                            argReturnValue = dbcolumn.IsHidden;
                            AssignValue(ref argReturnValue, r["IsHidden"]);
                            dbcolumn.IsHidden = Convert.ToBoolean(argReturnValue);
                            argReturnValue = dbcolumn.DataTypeName ;
                            AssignValue(ref argReturnValue, r["DataTypeName"]);
                            dbcolumn.DataTypeName = (string)argReturnValue;
                            argReturnValue = dbcolumn.ProviderSpecificDataType ;
                            AssignValue(ref argReturnValue, r["ProviderSpecificDataType"]);
                            dbcolumn.ProviderSpecificDataType = (Type)argReturnValue;

                            break;
                        default:
                            {
                                argReturnValue = dbcolumn.AllowDBNull;
                                AssignValue(ref argReturnValue, r["AllowDBNull"]);
                                dbcolumn.AllowDBNull = Convert.ToBoolean(argReturnValue);
                                argReturnValue = dbcolumn.AllowNull;
                                AssignValue(ref argReturnValue, dbcolumn.AllowDBNull);
                                dbcolumn.AllowNull = Convert.ToBoolean(argReturnValue);
                                argReturnValue = dbcolumn.BaseCatalogName;
                                AssignValue(ref argReturnValue, r["BaseCatalogName"]);
                                dbcolumn.BaseCatalogName = Convert.ToString(argReturnValue);
                                argReturnValue = dbcolumn.BaseColumnName;
                                AssignValue(ref argReturnValue, r["BaseColumnName"]);
                                dbcolumn.BaseColumnName = Convert.ToString(argReturnValue);
                                argReturnValue = dbcolumn.BaseTableName;
                                AssignValue(ref argReturnValue, r["BaseTableName"]);
                                dbcolumn.BaseTableName = Convert.ToString(argReturnValue);
                                argReturnValue = dbcolumn.BaseSchemaName;
                                AssignValue(ref argReturnValue, r["BaseSchemaName"]);
                                dbcolumn.BaseSchemaName = Convert.ToString(argReturnValue);
                                argReturnValue = dbcolumn.DataType;
                                AssignValue(ref argReturnValue, r["DataType"]);
                                dbcolumn.DataType = (Type)argReturnValue;
                                AssignValue(ref argReturnValue, r["DataTypeName"]);
                                dbcolumn.DataTypeName = (string)argReturnValue;
                                argReturnValue = dbcolumn.ColumnOrdinal;
                                AssignValue(ref argReturnValue, r["ColumnOrdinal"]);
                                dbcolumn.ColumnOrdinal = Convert.ToInt32(argReturnValue);
                                argReturnValue = dbcolumn.Size;
                                AssignValue(ref argReturnValue, r["ColumnSize"]);
                                dbcolumn.Size = Convert.ToInt32(argReturnValue);
                                argReturnValue = dbcolumn.NumericPrecision;
                                AssignValue(ref argReturnValue, r["NumericPrecision"]);
                                dbcolumn.NumericPrecision = Convert.ToInt32(argReturnValue);
                                argReturnValue = dbcolumn.NumericScale;
                                AssignValue(ref argReturnValue, r["NumericScale"]);
                                dbcolumn.NumericScale = Convert.ToInt32(argReturnValue);
                                argReturnValue = dbcolumn.IsUnique;
                                AssignValue(ref argReturnValue, r["IsUnique"]);
                                dbcolumn.IsUnique = Convert.ToBoolean(argReturnValue);
                                argReturnValue = dbcolumn.IsKey;
                                AssignValue(ref argReturnValue, r["IsKey"]);
                                dbcolumn.IsKey = Convert.ToBoolean(argReturnValue);
                                argReturnValue = dbcolumn.ProviderType;
                                AssignValue(ref argReturnValue, r["ProviderType"]);
                                dbcolumn.ProviderType = Convert.ToInt32(argReturnValue);
                                argReturnValue = dbcolumn.IsAutoIncrement;
                                AssignValue(ref argReturnValue, r["IsAutoIncrement"]);
                                dbcolumn.IsAutoIncrement = Convert.ToBoolean(argReturnValue);
                                argReturnValue = dbcolumn.IsRowVersion;
                                AssignValue(ref argReturnValue, r["IsRowVersion"]);
                                dbcolumn.IsRowVersion = Convert.ToBoolean(argReturnValue);
                                argReturnValue = dbcolumn.IsLong;
                                AssignValue(ref argReturnValue, r["IsLong"]);
                                dbcolumn.IsLong = Convert.ToBoolean(argReturnValue);
                                argReturnValue = dbcolumn.IsReadOnly;
                                AssignValue(ref argReturnValue, r["IsReadOnly"]);
                                dbcolumn.IsReadOnly = Convert.ToBoolean(argReturnValue);
                                break;
                            }


                            // Threading.Thread.Sleep(0)



                    }
                }
            }
            lobjConnection.Close();
            dt.Dispose();
            lobjConnection.Dispose();
            GC.Collect();


        }

        private void AssignValue(ref object ReturnValue, object Value)
        {

            if (!ReferenceEquals(Value, DBNull.Value))
            {
                ReturnValue = Value;
            }
        }

        public void ReloadAll()
        {
            bool Cancel = false;
            if (DisableEvents == false)
            {
                DataEventBefore?.Invoke(DataEventType.Query, ref Cancel);
            }

            if (Cancel == true)
                return;
            DoQuery();
            if (DisableEvents == false)
            {
                DataEventAfter?.Invoke(DataEventType.Query);
            }

            MoveFirst();
        }

        public void ResetDataTable()
        {
            bool Cancel = false;
            if (DisableEvents == false)
            {
                DataEventBefore?.Invoke(DataEventType.Query, ref Cancel);
            }

            if (Cancel == true)
                return;
            objDataTable.Reset();
            if (DisableEvents == false)
            {
                DataEventAfter?.Invoke(DataEventType.Query);
            }
        }

        public DataTable GetDataTable()
        {
            return objDataTable;
        }

        public void MoveFirst()
        {
            mBOF = false;
            mDataChanged = false;
            if (mAddNewStatus)
                return;
            bool Cancel = false;
            if (DisableEvents == false)
            {
                DataEventBefore?.Invoke(DataEventType.MoveFirst, ref Cancel);
            }

            if (Cancel == true)
                return;
            if (objDataTable.Rows.Count == 0)
            {
                mCurrentPosition = -1;
                mBOF = true;
                try
                {
                    if (mDataBinding == DataBoundControlsBehaviour.WindowsFormsDataBinding)
                    {
                        CurrencyManager.Position = -1;
                    }
                }
                catch
                {
                }

                switch (UseDataReader)
                {
                    case false:
                        {
                            DataRowGetValues(null, mDataBinding);
                            break;
                        }

                    case true:
                        {
                            DataRowGetValuesFromDataReader();
                            DataRowGetValues(null, mDataBinding);
                            break;
                        }
                }
            }
            else
            {
                try
                {
                    if (mDataBinding == DataBoundControlsBehaviour.WindowsFormsDataBinding)
                    {
                        CurrencyManager.Position = 0;
                    }

                    mCurrentPosition = 0;
                    if (UseDataReader == false)
                    {
                        DataRowGetValues(objDataTable.Rows[(int)mCurrentPosition], mDataBinding);
                    }
                    else
                    {
                        DataRowGetValuesFromDataReader();
                        DataRowGetValues(objDataTable.Rows[0], mDataBinding);
                    }

                    mBOF = false;
                }
                catch (Exception ex)
                {
                    HandleExceptions(ex);
                }
                // MsgBox(ex.Message)
                finally
                {
                }
            }

            if (DisableEvents == false)
            {
                DataEventAfter?.Invoke(DataEventType.MoveFirst);
            }
        }

        public void MoveNext()
        {
            mEOF = false;
            if (mAddNewStatus)
                return;
            bool Cancel = false;
            if (DisableEvents == false)
            {
                DataEventBefore?.Invoke(DataEventType.MoveNext, ref Cancel);
            }

            if (Cancel == true)
                return;
            try
            {
                switch (UseDataReader)
                {
                    case false:
                        {
                            if (mCurrentPosition > -1 & mCurrentPosition < objDataTable.Rows.Count - 1)
                            {
                                mCurrentPosition += 1;
                                DataRowGetValues(objDataTable.Rows[(int)mCurrentPosition], mDataBinding);
                                if (mDataBinding == DataBoundControlsBehaviour.WindowsFormsDataBinding)
                                {
                                    CurrencyManager.Position = mCurrentPosition;
                                }

                                mBOF = false;
                            }
                            else
                            {
                                mEOF = true;
                            }

                            mDataChanged = false;
                            break;
                        }

                    case true:
                        {
                            if (mCurrentPosition > -1)
                            {
                                if (objDataReader.Read())
                                {
                                    mCurrentPosition += 1;
                                    DataRowGetValuesFromDataReader();
                                    mEOF = false;
                                }
                                else
                                {
                                    mEOF = true;
                                }

                                if (!string.IsNullOrEmpty(LastError) | mEOF == true)
                                {
                                    mEOF = true;
                                }
                                else
                                {
                                    DataRowGetValues(objDataTable.Rows[0], mDataBinding);
                                    if (mDataBinding == DataBoundControlsBehaviour.WindowsFormsDataBinding)
                                    {
                                        CurrencyManager.Position = mCurrentPosition;
                                    }

                                    mEOF = false;
                                }
                            }
                            else
                            {
                                mEOF = true;
                            }

                            mDataChanged = false;
                            break;
                        }
                }

                if (DisableEvents == false)
                {
                    DataEventAfter?.Invoke(DataEventType.MoveNext);
                }
            }
            catch
            {
                //if (DisableEvents == false)
                //{
                //    DataEventAfter?.Invoke(DataEventType.MoveNext);
                //}
            }
        }

        public void MovePrevious()
        {
            if (UseDataReader == true)
                return;
            mBOF = true;
            if (mAddNewStatus)
                return;
            bool Cancel = false;
            if (DisableEvents == false)
            {
                DataEventBefore?.Invoke(DataEventType.MovePrevious, ref Cancel);
            }

            if (Cancel == true)
                return;
            try
            {
                if (mCurrentPosition > -1)
                {
                    mCurrentPosition -= 1;
                    if (mCurrentPosition < 0)
                    {
                        mCurrentPosition = 0;
                        mBOF = true;
                    }
                    else
                    {
                        mBOF = false;
                    }

                    DataRowGetValues(objDataTable.Rows[(int)mCurrentPosition], mDataBinding);
                    if (mDataBinding == DataBoundControlsBehaviour.WindowsFormsDataBinding)
                    {
                        CurrencyManager.Position = mCurrentPosition;
                    }
                }

                mDataChanged = false;
                if (DisableEvents == false)
                {
                    DataEventAfter?.Invoke(DataEventType.MovePrevious);
                }
            }
            catch
            {
            }
        }

        public void MoveLast()
        {
            if (UseDataReader == true)
                return;
            mEOF = false;
            mDataChanged = false;
            if (mAddNewStatus)
                return;
            bool Cancel = false;
            if (DisableEvents == false)
            {
                DataEventBefore?.Invoke(DataEventType.MoveLast, ref Cancel);
            }

            if (Cancel == true)
                return;
            int x;
            try
            {
                switch (UseDataReader)
                {
                    case false:
                        {
                            x = objDataTable.Rows.Count - 1;
                            DataRowGetValues(objDataTable.Rows[x], mDataBinding);
                            mCurrentPosition = x;
                            if (mDataBinding == DataBoundControlsBehaviour.WindowsFormsDataBinding)
                            {
                                CurrencyManager.Position = x;
                            }

                            mEOF = true;
                            if (DisableEvents == false)
                            {
                                DataEventAfter?.Invoke(DataEventType.MoveLast);
                            }

                            break;
                        }

                    case true:
                        {
                            break;
                        }
                }
            }
            catch
            {
            }

            if (DisableEvents == false)
            {
                DataEventAfter?.Invoke(DataEventType.MovePrevious);
            }
        }

        public void MoveTo(int Index)
        {
            mEOF = false;
            if (mAddNewStatus)
                return;
            if (UseDataReader == true)
                return;
            bool Cancel = false;
            if (DisableEvents == false)
            {
                DataEventBefore?.Invoke(DataEventType.MoveTo, ref Cancel);
            }

            if (Cancel == true)
                return;
            try
            {
                if (Index >= 0 & Index < objDataTable.Rows.Count)
                {
                    mCurrentPosition = Index;
                    DataRowGetValues(objDataTable.Rows[(int)mCurrentPosition], mDataBinding);
                    if (mDataBinding == DataBoundControlsBehaviour.WindowsFormsDataBinding)
                    {
                        CurrencyManager.Position = mCurrentPosition;
                    }

                    mBOF = false;
                }
                else
                {
                    mEOF = true;
                }

                mDataChanged = false;
            }
            catch
            {
            }

            if (DisableEvents == false)
            {
                DataEventAfter?.Invoke(DataEventType.MoveTo);
            }
        }

        public string SQLUpdateStmt
        {
            get
            {
                string SQLUpdateStmtRet = default;
                SQLUpdateStmtRet = SQLUpdate;
                return SQLUpdateStmtRet;
            }
        }

        public string SQLInsertStmt
        {
            get
            {
                string SQLInsertStmtRet = default;
                SQLInsertStmtRet = SQLInsert;
                return SQLInsertStmtRet;
            }
        }

        public string SQLDeleteStmt
        {
            get
            {
                string SQLDeleteStmtRet = default;
                SQLDeleteStmtRet = SQLDelete;
                return SQLDeleteStmtRet;
            }
        }

        public string SQLSelectStmt
        {
            get
            {
                string SQLSelectStmtRet = default;
                SQLSelectStmtRet = SQLSelect;
                return SQLSelectStmtRet;
            }
        }

        public string SQLWhereConditions
        {
            get
            {
                string SQLWhereConditionsRet = default;
                SQLWhereConditionsRet = SQLWhere;
                return SQLWhereConditionsRet;
            }
        }

        public void BuildSQLDelete(ref DbParameterCollection DbParameters)
        {
            //int i = 0;
            string SQL2 = "";
            //string Prefix = "";
            string parametername = "";
            DbParameters.Clear();
            var sSQL2 = new StringBuilder();
            string SS = "";
            // 
            foreach (DbColumn dbColumn in mDbColumns)
            {
                if (dbColumn.IsPrimaryKey)
                {
                    SS = dbColumn.ColumnOrdinal.ToString();
                    parametername = mDbConfig.ParameterNamePrefixE + SS + "_ShadowValue";
                    switch (mDbConfig.Provider)
                    {
                        case Providers.SqlServer:
                            {
                                sSQL2.Append(dbColumn.DbColumnName + "=" + parametername);
                                break;
                            }

                        case Providers.DB2_iSeries :
                            {
                                sSQL2.Append(dbColumn.DbColumnName + "=" + parametername);
                                break;
                            }

                        case Providers.MySQL:
                            {
                                sSQL2.Append(dbColumn.DbColumnName + "=" + parametername);
                                break;
                            }
                        case Providers.OleDb or Providers.OleDb_DB2:
                            {
                                sSQL2.Append(dbColumn.DbColumnName + "=?");
                                break;
                            }

                        case Providers.ODBC or Providers.ODBC_DB2:
                            {
                                sSQL2.Append(dbColumn.DbColumnName + "=?");
                                break;
                            }
                        case Providers.OracleDataAccess:
#if COMPILEORACLE
                            sSQL2.Append(dbColumn.DbColumnName + "=" + parametername);
#endif
                            break;

                        case Providers.ConfigDefined:
                            {
                                sSQL2.Append(dbColumn.DbColumnName + "=?");
                                break;
                            }

                        default:
                            sSQL2.Append(dbColumn.DbColumnName + "=?");
                            break;
                    }

                    sSQL2.Append(" AND ");
                    var p2 = objFactory.CreateParameter();
                    switch (mDbConfig.Provider)
                    {
                        case Providers.OracleDataAccess:
                            if (dbColumn.DbType == DbType.Time)
                            {
                                p2.DbType = DbType.String;
                            }
                            else
                            {
                                p2.DbType = dbColumn.DbType;
                            }
                            break;

                        case Providers.SqlServer:

                            System.Data.SqlClient.SqlParameter _p2;
                            _p2 = (System.Data.SqlClient.SqlParameter)p2;

                            if (dbColumn.DbType == DbType.Time)
                            {
                                _p2.SqlDbType = SqlDbType.Time;

                            }
                            else
                            {
                                _p2.DbType = dbColumn.DbType;
                            }
                            break;

                        default:
                            {
                                p2.DbType = dbColumn.DbType;
                                break;
                            }
                    }

                    p2.SourceColumn = dbColumn.DbColumnName;
                    p2.ParameterName = parametername;
                    DbParameters.Add(p2);
                }
            }

            SQL2 = sSQL2.ToString();
            try
            {
                SQL2 = SQL2.Substring(0, SQL2.Length - 4);
            }
            catch
            {
            }

            EnableBoundControls();
            var sSQLDelete = new StringBuilder();
            sSQLDelete.Append("DELETE FROM ");
            sSQLDelete.Append(DbTableName);
            sSQLDelete.Append(" WHERE (");
            sSQLDelete.Append(SQL2 + ")");
            SQLDelete = sSQLDelete.ToString();
        }

        public void DataReaderGetValues(DbDataReader DataReader)
        {
            bool Cancel = false;
            if (DisableEvents == false)
            {
                DataEventBefore?.Invoke(DataEventType.BindingFromDataReader, ref Cancel);
            }

            if (Cancel == true)
                return;

            System.Threading.Tasks.Parallel.For(0, mDbColumns.Count, _iColumn =>
                {
                    DbColumn dbcolumn = mDbColumns.get_Item(_iColumn);
                    dbcolumn.Value = DataReader[dbcolumn.DbColumnName];
                    dbcolumn.OriginalValue = dbcolumn.Value;
                });
            if (DisableEvents == false)
            {
                DataEventAfter?.Invoke(DataEventType.BindingFromDataReader);
            }
        }

        public void DeleteAll(bool Requery = true)
        {
            if (UseDataReader == true)
                return;
            var Cancel = default(bool);
            if (DisableEvents == false)
            {
                DataEventBefore?.Invoke(DataEventType.DeleteAll, ref Cancel);
            }

            if (Cancel == true)
                return;

            DataSet Ds;
            if (Requery == true)
            {
                Ds = ExecuteDataSet();
            }
            else
            {
                Ds = objDataSet;
            }

            var loopTo = Ds.Tables[0].Rows.Count - 1;
            for (int i = 0; i <= loopTo; i++)
                Ds.Tables[0].Rows[i].Delete();
            DeleteFromDataSet(ref Ds);
            if (DisableEvents == false)
            {
                DataEventAfter?.Invoke(DataEventType.DeleteAll);
            }
        }

        #region Delete Method
        public int Delete()
        {
            return Delete(mDataBinding);
        }

        public object Remove()
        {
            return Remove(DataBinding);
        }

        public int Remove(DataBoundControlsBehaviour UpdateBoundControls)
        {
            int RemoveRet = default;
            ExecutionResult.Context = "DbObject:Remove";
            RemoveRet = 0;
            if (UseDataReader == true)
                return RemoveRet;
            if (mIsReadOnly == true)
                return RemoveRet;
            switch (DbObjectType)
            {
                case DbObjectTypeEnum.Table or DbObjectTypeEnum.View or DbObjectTypeEnum.SQLQuery:
                    {
                        break;
                    }

                default:
                    {
                        return RemoveRet;
                    }
            }

            if (mDbObjectType != DbObjectTypeEnum.Table)
                return RemoveRet;
            if (mAddNewStatus == true)
                return RemoveRet;
            bool Cancel = false;
            if (DisableEvents == false)
            {
                DataEventBefore?.Invoke(DataEventType.Delete, ref Cancel);
            }

            if (Cancel == true)
            {
                return RemoveRet;
            }

            if (UpdateBoundControls == DataBoundControlsBehaviour.BasicDALDataBinding & mCurrentPosition < 0L)
            {
                BlankDbColumns(UpdateBoundControls);
                return RemoveRet;
            }

            if (CurrentDataRow is object)
            {
                try
                {
                    CurrentDataRow.Delete();
                    RemoveRet = 1;
                }
                catch
                {
                    RemoveRet = 0;
                }
            }

            mAddNewStatus = false;
            if (DisableEvents == false)
            {
                DataEventAfter?.Invoke(DataEventType.Delete);
            }

            return RemoveRet;
        }

        public int Delete(DataBoundControlsBehaviour UpdateBoundControlsBehaviour)
        {
            ExecutionResult.Context = "DbObject:Delete";
            if (UseDataReader == true)
                return default;
            if (mIsReadOnly == true)
                return default;
            switch (DbObjectType)
            {
                case DbObjectTypeEnum.Table or DbObjectTypeEnum.View or DbObjectTypeEnum.SQLQuery:
                    {
                        break;
                    }

                default:
                    {
                        return default;
                    }
            }

            DbCommand lobjCommand;
            // Dim UseCachedConnection As Boolean = False
            int i = -1;
            if (mDbObjectType != DbObjectTypeEnum.Table)
                return default;
            if (mAddNewStatus == true)
                return default;
            bool Cancel = false;
            if (DisableEvents == false)
            {
                DataEventBefore?.Invoke(DataEventType.Delete, ref Cancel);
            }

            if (Cancel == true)
                return default;
            if (UpdateBoundControlsBehaviour == DataBoundControlsBehaviour.BasicDALDataBinding & mCurrentPosition < 0L)
            {
                BlankDbColumns(UpdateBoundControlsBehaviour);
                return default;
            }



            

            if (UpdateBatchEnabled == false)
            {
                lobjCommand = objDeleteCommand;

                System.Threading.Tasks.Parallel.For(0, mDbColumns.Count, _iColumn =>
                    {
                        DbColumn dbcolumn = mDbColumns.get_Item(_iColumn);// (DbColumn)mDbColumns.ElementAtOrDefault(_iColumn);
                        DeleteDbColumns(ref dbcolumn, ref lobjCommand);
                    });

                try
                {
                    lobjCommand.CommandText = SQLDelete;
                    if (DbConnection.State == System.Data.ConnectionState.Closed)
                    {
                        DbConnection.Open();
                    }

                        ExecutionResult.Context = "DbObject:Delete.ExecuteNonQuery";
                        if (lobjCommand.Transaction is null & DbConfig.DbTransaction is object)
                        {
                            switch (mDbConfig.Provider)
                            {
                                case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                                    {
                                        lobjCommand.Transaction = DbConfig.DbTransaction;
                                        break;
                                    }

                                default:
                                    {
                                        lobjCommand.Transaction = DbConfig.DbTransaction;
                                        break;
                                    }
                            }
                        }

                    i = lobjCommand.ExecuteNonQuery();
                  

                    ExecutionResult.Context = "DbObject:Delete.UpdateBoundControls";
                    if (UpdateBoundControlsBehaviour != DataBoundControlsBehaviour.NoDataBinding)
                    {
                        objDataTable.Rows[(int)mCurrentPosition].Delete();
                        objDataTable.AcceptChanges();
                        if (mCurrentPosition >= objDataTable.Rows.Count)
                        {
                            mCurrentPosition = mCurrentPosition - 1;
                        }

                        if (mCurrentPosition == -1)
                        {
                            DataRowGetValues(null, mDataBinding);
                        }
                        else
                        {
                            DataRowGetValues(objDataTable.Rows[(int)mCurrentPosition], mDataBinding);
                        }
                    }

                    if (mDataBinding == DataBoundControlsBehaviour.WindowsFormsDataBinding)
                    {
                        CurrencyManager.Position = mCurrentPosition;
                    }
                }
                catch (Exception ex)
                {
                    HandleExceptions(ex, "DELETE");
                }
                finally
                {
                    // If UseCachedConnection = False Then lobjConnection.Close()
                }
            }
            else
            {
                ExecutionResult.Context = "DbObject:Delete.UpdateBatch";
                // BATCHENABLED = TRUE
                int AffectedRows = 0;
                objDataTable.Rows[(int)mCurrentPosition].Delete();
                var argDataTable = objDataTable;
                AffectedRows = UpdateFromDataTable(ref argDataTable);
                objDataTable = argDataTable;
                if (AffectedRows == 1)
                {
                    if (mCurrentPosition > 0L)
                    {
                        mCurrentPosition = mCurrentPosition - 1;
                    }
                }

                objDataTable.AcceptChanges();
                if (UpdateBoundControlsBehaviour != DataBoundControlsBehaviour.NoDataBinding)
                {
                    if (mCurrentPosition == -1)
                    {
                        DataRowGetValues(null, mDataBinding);
                    }
                    else
                    {
                        DataRowGetValues(objDataTable.Rows[(int)mCurrentPosition], mDataBinding);
                    }
                }

                if (mDataBinding == DataBoundControlsBehaviour.WindowsFormsDataBinding)
                {
                    CurrencyManager.Position = mCurrentPosition;
                }
            }

            mAddNewStatus = false;
            mRowCount = objDataTable.Rows.Count;
            if (DisableEvents == false)
            {
                DataEventAfter?.Invoke(DataEventType.Delete);
            }
            if (mDbConfig.DbConnectionKeepOpen == false)
                DbConnection.Close();
            return i;
        }

        private void DeleteDbColumns(ref DbColumn DbColumn, ref DbCommand lobjCommand)
        {
            object zValue;
            if (DbColumn.OriginalValue is null)
            {
                zValue = DBNull.Value;
            }
            else
            {
                zValue = DbColumn.OriginalValue;
            }

            try
            {
                if (DbColumn.IsPrimaryKey)
                {
                    // lobjCommand.Parameters(Me.DbParameterNamePrefix + dbcolumn.ColumnOrdinal.ToString + "_ShadowValue").Value = zValue
                    lobjCommand.Parameters[mDbConfig.ParameterNamePrefixE + DbColumn.ColumnOrdinal.ToString() + "_ShadowValue"].Value = zValue;
                }
            }
            catch
            {
            }
        }



        #endregion
        public int AddNew(bool SetDefaultValues = true)
        {
            ExecutionResult.Context = "DbObject:AddNew";
            AddNew(mDataBinding, SetDefaultValues);
            return default;
        }

        public int AddNew(DataBoundControlsBehaviour mDataBinding, bool SetDefaultValues = true)
        {
            int AddNewRet = default;
            ExecutionResult.Context = "DbObject:AddNew";
            AddNewRet = 0;
            if (mAddNewStatus == true)
                return AddNewRet;
            // If Me.DbObjectIsValid = False Then Exit Function

            if (UseDataReader == true)
                return AddNewRet;
            if (mIsReadOnly == true)
                return AddNewRet;
            switch (DbObjectType)
            {
                case DbObjectTypeEnum.Table or DbObjectTypeEnum.View or DbObjectTypeEnum.SQLQuery:
                    {
                        break;
                    }

                default:
                    {
                        return AddNewRet;
                    }
            }

            //string ParameterName = "";
            DataRow Row;
            bool Cancel = false;
            if (DisableEvents == false)
            {
                DataEventBefore?.Invoke(DataEventType.AddNew, ref Cancel);
            }

            if (Cancel == true)
                return AddNewRet;
            mAddNewStatus = true;
            if (mCurrentPosition > -1)
            {
                Row = objDataTable.NewRow();
                objDataTable.Rows.InsertAt(Row, (int)mCurrentPosition);
            }
            else
            {
                Row = objDataTable.Rows.Add();
                mCurrentPosition = 0;
            }

            mRowCount = objDataTable.Rows.Count;
            CurrentDataRow = Row;
            if (mDataBinding == DataBoundControlsBehaviour.WindowsFormsDataBinding)
            {
                CurrencyManager.Position = mCurrentPosition;
            }

            if (mDataBinding == DataBoundControlsBehaviour.BasicDALDataBinding)
            {
                InitBoundControls();
            }

            if (SetDefaultValues)
            {
                if (UseParallelism == false)
                {

                    for (int i = 0; i < mDbColumns.Count; i++)
                    {
                        DbColumn tmp = mDbColumns.get_Item(i);
                        AddNewLoopDbColumn(ref tmp);
                        mDbColumns.set_Item(i, tmp);
                    }
                }
                else
                {
                    System.Threading.Tasks.Parallel.For(0, mDbColumns.Count, _iColumn =>
                    {
                        DbColumn _dbcolumn = mDbColumns.get_Item(_iColumn);// (DbColumn)mDbColumns.ElementAtOrDefault(_iColumn);
                        AddNewLoopDbColumn(ref _dbcolumn);
                    });
                }
            }

            if (DisableEvents == false)
            {
                DataEventAfter?.Invoke(DataEventType.AddNew);
            }

            return AddNewRet;
        }

        private void AddNewLoopDbColumn(ref DbColumn DbColumn)
        {
            try
            {
                DbColumn.Value = DbColumn.DefaultValue;
                DbColumn.OriginalValue = DbColumn.DefaultValue;
            }
            catch
            {
            }

            switch (mDataBinding)
            {
                case DataBoundControlsBehaviour.BasicDALDataBinding:
                    {
                        foreach (BoundControl BoundControl in DbColumn.BoundControls)
                        {
                            try
                            {
                                Interaction.CallByName(BoundControl.Control, BoundControl.PropertyName, CallType.Set, DbColumn.DefaultValue);

                            }
                            catch
                            {
                            }
                        }

                        break;
                    }

                case DataBoundControlsBehaviour.WindowsFormsDataBinding:
                    {
                        break;
                    }

                default:
                    {
                        break;
                    }
            }
        }
        #region Insert Method

        public int Insert(DataBoundControlsBehaviour UpdateBoundControlsBehaviour = DataBoundControlsBehaviour.NoDataBinding)
        {
            ExecutionResult.Context = "DbObject:Insert";
            if (mIsReadOnly == true)
                return default;

            switch (DbObjectType)
            {
                case DbObjectTypeEnum.Table or DbObjectTypeEnum.View or DbObjectTypeEnum.SQLQuery:
                    {
                        break;
                    }

                default:
                    {
                        return default;
                    }
            }

            bool Cancel = false;
            if (DisableEvents == false)
            {
                DataEventBefore?.Invoke(DataEventType.Insert, ref Cancel);
            }

            if (Cancel == true)
                return default;


            DbCommand lobjCommand;
            DataRow Row = null;
            int i = -1;

            lobjCommand = objInsertCommand;

            if (Fields == null)
            {
                Fields = GetFields(this);
            }

            if (objDataTable.Rows.Count > 0)
            {
                Row = objDataTable.Rows[(int)mCurrentPosition];
            }
            else
            {
                mAddNewStatus = false;
                return 0;
            }

            if (UpdateBoundControlsBehaviour == DataBoundControlsBehaviour.WindowsFormsDataBinding)
            {
                CurrencyManager.EndCurrentEdit();
            }

            if (DataBinding != DataBoundControlsBehaviour.NoDataBinding)
            {
                if (UseParallelism)
                {

                    System.Threading.Tasks.Parallel.For(0, mDbColumns.Count, (_iColumn) =>
                        {
                            DbColumn _dbColumn = mDbColumns.get_Item(_iColumn);
                            InsertDbColumnsDataBinding(ref Row, ref _dbColumn, ref lobjCommand, UpdateBoundControlsBehaviour);
                        });
                }
                else
                {

                    for (int _i = 0; _i < mDbColumns.Count; _i++)
                    {
                        DbColumn _dbColumn = mDbColumns.get_Item(_i);
                        InsertDbColumnsDataBinding(ref Row, ref _dbColumn, ref lobjCommand, UpdateBoundControlsBehaviour);
                        mDbColumns.set_Item(_i, _dbColumn);
                    }

                }
            }


            // ----- fine codice originario

            // NON CI SONO CONTROLLI BINDATI
            else if (UpdateBatchEnabled == false)
            {
                // ' CODICE PARALLELO
                System.Threading.Tasks.Parallel.For(0, mDbColumns.Count, (_iColumn) =>
                {
                    DbColumn _iDbColumn = mDbColumns.get_Item(_iColumn);
                    InsertDbColumnsNoDataBinding(ref _iDbColumn, ref lobjCommand);
                });
            }

            // CR GDG05092018
            if (UpdateBatchEnabled == true)
            {
                mAddNewStatus = false;
                mRowCount = objDataTable.Rows.Count;
                if (DisableEvents == false)
                {
                    DataEventAfter?.Invoke(DataEventType.Update);
                }
                return 1;
            }

            objDataTable.AcceptChanges();
            // modifica per tabelle SQL con Identity
            bool bExecuteScalar = false;
            bool bExecuteMulti = false;
            string SQLNoCount = "";
            string SQLSelectScopeIdentity = "";
            string sSQLInsert = "";

            switch (mDbConfig.Provider)
            {
                case Providers.SqlServer:
                    {
                        if (mIdentityDBColumn is object)
                        {
                            // SQLNoCount = "SET NOCOUNT ON; "
                            SQLSelectScopeIdentity = "SELECT SCOPE_IDENTITY() AS NEWIDVALUE; ";
                            bExecuteScalar = true;
                            bExecuteMulti = true;
                        }

                        else
                        {
                        }

                        break;
                    }

                default:
                    {
                        break;
                    }
            }

            sSQLInsert = SQLInsert;
            try
            {
                lobjCommand.CommandText = sSQLInsert;
                // lobjCommand.Prepare()
                if (DbConnection.State == System.Data.ConnectionState.Closed)
                {
                    DbConnection.Open();
                }

                //If lobjCommand.Transaction Is Nothing And Me.DbConfig.DbTransaction IsNot Nothing Then
                if (lobjCommand.Transaction == null & DbConfig.DbTransaction != null)
                {
                    switch (mDbConfig.Provider)
                    {
                        case Providers.ODBC_DB2:
                        case Providers.OleDb_DB2:
                            {
                                lobjCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }

                        default:
                            {
                                lobjCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }
                    }
                }

                switch (bExecuteScalar)
                {
                    case true:
                        {
                            if (bExecuteMulti == true)
                            {
                                i = Convert.ToInt32(lobjCommand.ExecuteScalar());
                            }
                            else
                            {
                                i = lobjCommand.ExecuteNonQuery();
                                lobjCommand.CommandText = SQLSelectScopeIdentity;
                                i = Convert.ToInt32(lobjCommand.ExecuteScalar());
                            }

                            mIdentityDBColumn.Value = Utilities.DbValueCast(i, mIdentityDBColumn.DbType, NullDateValue, mIdentityDBColumn.DateTimeResolution);
                            UpdateBoundControls(mIdentityDBColumn);
                            break;
                        }

                    default:
                        {
                            i = lobjCommand.ExecuteNonQuery();
                            break;
                        }
                }

                if (i > 0)
                    mAddNewStatus = false;
            }
            catch (Exception ex)
            {
                HandleExceptions(ex, "INSERT");
                i = 0;
            }
            finally
            {
            }

            if (DisableEvents == false)
            {
                DataEventAfter?.Invoke(DataEventType.Insert);
            }
            if (mDbConfig.DbConnectionKeepOpen == false)
                DbConnection.Close();
            return i;
        }

        private void InsertDbColumnsDataBinding(ref DataRow Row, ref DbColumn _DbColumn, ref DbCommand lobjCommand, DataBoundControlsBehaviour UpdateBoundControlsBehaviour)
        {
            object value = null;

            object xValue = null;
            object zValue = null;
            object rowvalue = null;
            if (UpdateBoundControlsBehaviour == DataBoundControlsBehaviour.WindowsFormsDataBinding)
            {
                _DbColumn.Value = Row[_DbColumn.DbColumnNameE];
                _DbColumn.OriginalValue = _DbColumn.Value;
            }

            if (UpdateBoundControlsBehaviour == DataBoundControlsBehaviour.BasicDALDataBinding)
            {
                foreach (BoundControl _BoundControl in _DbColumn.BoundControls)
                {
                    switch (_BoundControl.BindingBehaviour)
                    {
                        case BasicDAL.BindingBehaviour.ReadWrite:
                        case BasicDAL.BindingBehaviour.ReadWriteAddNew:
                        case BasicDAL.BindingBehaviour.ReadAddNew:
                            {
                                value = Interaction.CallByName(_BoundControl.Control, _BoundControl.PropertyName, CallType.Get);
                                //value = Utilities.CallByName(_BoundControl.Control, _BoundControl.PropertyName, Utilities.CallType.Get);
                                _DbColumn.Value = Utilities.DbValueCast(value, _DbColumn.DbType, NullDateValue, _DbColumn.DateTimeResolution);
                                rowvalue = _DbColumn.Value;
                                _DbColumn.OriginalValue = _DbColumn.Value;
                                try
                                {

                                    switch (mDbConfig.Provider)
                                    {
                                        case Providers.OracleDataAccess:
                                            Row[_DbColumn.DbColumnNameE.Replace("\"", string.Empty)] = _DbColumn.Value;
                                            break;

                                        default:
                                            {
                                                Row[_DbColumn.DbColumnNameE] = _DbColumn.Value;
                                                break;
                                            }
                                    }
                                }
                                catch
                                {
                                }

                                break;
                            }

                        default:
                            {
                                break;
                            }
                    }
                }
                // controlli su valori NULL della colonna
                xValue = null;
                zValue = null;
                if (_DbColumn.Value is null)
                {
                    xValue = DBNull.Value;
                }
                else
                {
                    xValue = _DbColumn.Value;
                }

                if (_DbColumn.OriginalValue is null)
                {
                    zValue = DBNull.Value;
                }
                else
                {
                    zValue = _DbColumn.OriginalValue;
                }


                string ParameterName = string.Concat(mDbConfig.ParameterNamePrefixE, _DbColumn.ColumnOrdinal.ToString());
                //string SS = _DbColumn.ColumnOrdinal.ToString();
                try
                {
                    lobjCommand.Parameters[ParameterName].Value = ConvertParameterValue(_DbColumn, xValue);
                }
                catch
                {
                }

                try
                {
                    if (_DbColumn.IsPrimaryKey == true)
                    {
                        lobjCommand.Parameters[ParameterName + "_ShadowValue"].Value = ConvertParameterValue(_DbColumn, zValue);
                    }
                }
                catch
                {
                }



            }
        }

        private void InsertDbColumnsNoDataBinding(ref DbColumn DbColumn, ref DbCommand lobjCommand)
        {
            if (DbColumn.Value == null)
            {
                DbColumn.Value = DbColumn.DefaultValue;
            }

            DbColumn.OriginalValue = DbColumn.Value;

            string ParameterName = string.Concat(mDbConfig.ParameterNamePrefixE, DbColumn.ColumnOrdinal.ToString());
            if (DbColumn.IsReadOnly == false)
            {
                lobjCommand.Parameters[ParameterName].Value = ConvertParameterValue(DbColumn);
            }


        }



        private object ConvertParameterValue(DbColumn DbColumn, object Value = null)
        {
            ExecutionResult.Context = "DbObject:ConvertParameterValue";
            object _value = DBNull.Value;
            //bool esito = false;
            if (Value is DBNull == false)
            {
                if (Value is null)
                {
                    _value = Utilities.DbValueCast(DbColumn.Value, DbColumn.DbType, NullDateValue, DbColumn.DateTimeResolution);
                }
                else
                {
                    _value = Value;
                }
            }


            //if (DbColumn.DbColumnNameE == "ActionOnSourceLocationInventory")
            //{
            //    int zz = 0;

            //}

            Utilities.DBNullHandler(ref _value);
            if (_value is DBNull == false)
            {
                switch (DbColumn.DbType)
                {
                    case DbType.AnsiString or DbType.AnsiStringFixedLength or DbType.String or DbType.StringFixedLength:

                        if (DbColumn.Size > 0)
                        {
                            if (_value.ToString().Length > DbColumn.Size)
                            {
                                //_value = Strings.Mid(Conversions.ToString(_value), 1, DBCOlumn.Size);
                                _value = _value.ToString().Substring(0, DbColumn.Size);
                            }
                        }
                        break;

                    case DbType.Date or DbType.DateTime or DbType.DateTime2:

                        switch (DbColumn.DbObject.DbConfig.Provider)
                        {
                            case Providers.SqlServer:

                                if (_value == null)
                                {
                                    _value = System.Data.SqlTypes.SqlDateTime.Null;
                                }
                                break;

                            default:
                                break;

                        }
                        break;

                    case DbType.Boolean:
                        break;

                    case DbType.Time:
                        switch (mDbConfig.Provider)
                        {
                            case Providers.ODBC_DB2 or Providers.ODBC:
                                TimeSpan ts = DateTime.Now.TimeOfDay;
                                bool success;
                                success = TimeSpan.TryParse(Convert.ToString(_value), out ts);
                                _value = ts;
                                break;

                            case Providers.OracleDataAccess:
#if COMPILEORACLE
                                TimeSpan tsoracle = new TimeSpan();
                                tsoracle = System.Convert.ToDateTime(_value).TimeOfDay;
                                if ((tsoracle == new TimeSpan()) == false)
                                {
                                    var OracleInterval = new Oracle.ManagedDataAccess.Types.OracleIntervalDS(tsoracle.Days, tsoracle.Hours, tsoracle.Minutes, tsoracle.Seconds, tsoracle.Milliseconds);
                                    _value = OracleInterval.ToString();
                                }
#endif
                                break;

                            default:
                                break;
                        }
                        break;

                    default:
                        break;
                }
            }
            return _value;
        }

        #endregion

        public void BuildSQLInsert(ref DbParameterCollection DBParameters)
        {
            ExecutionResult.Context = "DbObject:BuildSQLInsert";
            string SQL1 = "";
            string SQL2 = "";
            StringBuilder sSQL1 = new StringBuilder();
            StringBuilder sSQL2 = new StringBuilder();
            string parametername = "";
            string SS = "";
            DBParameters.Clear();
            System.Data.Common.DbParameter p1;
            foreach (DbColumn DbColumn in mDbColumns)
            {
                // If dbcolumn.IsIdentity = False Then
                if (DbColumn.IsReadOnly == false)
                {
                    SS = DbColumn.ColumnOrdinal.ToString();
                    parametername = mDbConfig.ParameterNamePrefixE + SS;
                    p1 = objFactory.CreateParameter();
                    p1.ParameterName = parametername;
                    p1.DbType = DbColumn.DbType;
                    p1.SourceColumn = DbColumn.DbColumnName;
                    p1.Direction = ParameterDirection.Input;

                    switch (mDbConfig.Provider)
                    {

                        case Providers.OracleDataAccess:
                            System.Data.SqlClient.SqlParameter _pOracle;
                            _pOracle = (System.Data.SqlClient.SqlParameter)p1;

                            if (DbColumn.DbType == DbType.Time)
                                _pOracle.DbType = DbType.String;
                            break;


                        case Providers.SqlServer:


                            System.Data.SqlClient.SqlParameter _pSql;
                            _pSql = (System.Data.SqlClient.SqlParameter)p1;

                            if (DbColumn.DbType == DbType.Time)
                            {
                                _pSql.SqlDbType = SqlDbType.Time;

                            }
                            else
                            {
                                _pSql.DbType = DbColumn.DbType;
                            }
                            break;

                        case Providers.OleDb_DB2 or Providers.OleDb:

                            System.Data.OleDb.OleDbParameter _pOleDb;
                            _pOleDb = (System.Data.OleDb .OleDbParameter)p1;

                            _pOleDb.DbType = DbColumn.DbType;
                            
                            break;

                        case Providers.ODBC  or Providers.ODBC_DB2 :

                            System.Data.Odbc .OdbcParameter _pOdbc;
                            _pOdbc = (System.Data.Odbc.OdbcParameter)p1;

                            _pOdbc .DbType = DbColumn.DbType;

                            break;

                        case Providers.DB2_iSeries :

                            IBM .Data .DB2 .iSeries .iDB2Parameter  _pDB2_Iseries;
                            _pDB2_Iseries = (IBM.Data.DB2.iSeries.iDB2Parameter)p1;

                            _pDB2_Iseries.DbType = DbColumn.DbType;

                            break;

                        case Providers.MySQL :

                            MySql .Data .MySqlClient .MySqlParameter  _pMySQL;
                            _pMySQL = (MySql.Data.MySqlClient.MySqlParameter)p1;

                            _pMySQL.DbType = DbColumn.DbType;

                            break;
                        default:
                            {
                                p1.DbType = DbColumn.DbType;
                                break;
                            }
                    }

                    p1.Size = DbColumn.Size;
                    DBParameters.Add(p1);
                    sSQL1.Append(DbColumn.DbColumnName + ", ");
                    switch (mDbConfig.Provider)
                    {
                        case Providers.SqlServer:
                            {
                                sSQL2.Append(parametername + ", ");
                                break;
                            }

                        case Providers.DB2_iSeries :
                            {
                                sSQL2.Append(parametername + ", ");
                                break;
                            }

                        case Providers.MySQL:
                            {
                                sSQL2.Append(parametername + ", ");
                                break;
                            }
                        case Providers.OleDb or Providers.OleDb_DB2:
                            {
                                // QUALIFIED?
                                sSQL2.Append("?, ");
                                //sSQL2.Append(parametername+", ");
                                break;
                            }

                        case Providers.ODBC or Providers.ODBC_DB2:
                            {
                                sSQL2.Append("?, ");
                                //sSQL2.Append(parametername + ", ");
                                break;
                            }

                        case Providers.OracleDataAccess:
                            {
                                sSQL2.Append(parametername + ", ");
                                break;
                            }
                        case Providers.ConfigDefined:
                            {
                                sSQL2.Append(parametername + ",");
                                break;
                            }
                    }
                }
            }

            SQL1 = sSQL1.ToString();
            SQL2 = sSQL2.ToString();
            SQL1 = SQL1.Substring(0, SQL1.Length - 2);
            SQL2 = SQL2.Substring(0, SQL2.Length - 2);
            StringBuilder sSQLInsert = new StringBuilder();
            switch (mDbConfig.Provider)
            {
                case Providers.SqlServer:
                    {
                        if (mIdentityDBColumn is object)
                        {
                            sSQLInsert.Append("SET NOCOUNT ON;");
                        }

                        break;
                    }

                default:
                    {
                        break;
                    }
            }

            sSQLInsert.Append("INSERT INTO ");
            sSQLInsert.Append(DbTableName);
            sSQLInsert.Append(" (");
            sSQLInsert.Append(SQL1);
            sSQLInsert.Append(") VALUES(");
            sSQLInsert.Append(SQL2);
            //sSQLInsert.Append(")");

            switch (mDbConfig.Provider)
            {
                case Providers.SqlServer:
                    sSQLInsert.Append(");");
                    break;

                default:
                    sSQLInsert.Append(")");
                    break;
                    
            }

            
            switch (mDbConfig.Provider)
            {
                case Providers.SqlServer:
                    {
                        if (mIdentityDBColumn is object)
                        {
                            sSQLInsert.Append("SELECT SCOPE_IDENTITY() AS NEWIDVALUE;");
                        }

                        break;
                    }

                default:
                    {
                        break;
                    }
            }

            SQLInsert = sSQLInsert.ToString();
        }

        private string mDbFunctionName;

        public string DbFunctionName
        {
            get
            {
                string DbFunctionNameRet = default;
                ExecutionResult.Context = "DbObject:DbFunctionName Get";
                DbFunctionNameRet = mDbFunctionName;
                return DbFunctionNameRet;
            }

            set
            {

                ExecutionResult.Context = "DbObject:DbFunctionName Set";
                if (mDbConfig.SupportSquareBrackets)
                {
                    if (value != null)
                    {

                        if (value.ToString().StartsWith("[") == false)
                            value = "[" + value;
                        if (value.ToString().EndsWith("]") == false)
                            value = value + "]";
                    }
                }

                mDbFunctionName = value.ToString();
            }
        }

        private string mDbStoredProcedureName;

        public string DbStoredProcedureName
        {
            get
            {
                string DbStoredProcedureNameRet = default;
                ExecutionResult.Context = "DbObject:DbStoredProcedureName Get";
                DbStoredProcedureNameRet = mDbStoredProcedureName;
                return DbStoredProcedureNameRet;
            }

            set
            {
                ExecutionResult.Context = "DbObject:DbStoredProcedure Set";
                if (mDbConfig.SupportSquareBrackets == true)
                {
                    if (!string.IsNullOrEmpty(value))
                    {
                        if (value.ToString().StartsWith("[") == false)
                            value = "[" + value;
                        if (value.ToString().EndsWith("]") == false)
                            value = value + "]";
                    }
                }

                mDbStoredProcedureName = value;
                mDbTableName = value;
            }
        }

        public string DbTableName
        {
            get
            {
                string DbTableNameRet = default;
                ExecutionResult.Context = "DbObject:DbTableName Get";
                DbTableNameRet = mDbTableName;
                return DbTableNameRet;
            }

            set
            {
                ExecutionResult.Context = "DbObject:DbFTableName Set";
                if (mDbConfig.SupportSquareBrackets == true)
                {
                    if (!string.IsNullOrEmpty(value))
                    {
                        if (value.ToString().StartsWith("[") == false)
                            value = "[" + value;
                        if (value.ToString().EndsWith("]") == false)
                            value = value + "]";
                    }
                }

                mDbTableName = value;
            }
        }

        public DbFiltersGroup FiltersGroup
        {
            get
            {
                DbFiltersGroup FiltersGroupRet = default;
                ExecutionResult.Context = "DbObject:DbFilterGroup Get";
                FiltersGroupRet = objFiltersGroup;
                return FiltersGroupRet;
            }

            set
            {
                ExecutionResult.Context = "DbObject:DbFilterGroup Set";
                objFiltersGroup = value;
            }
        }

        private void BuildSQLUpdate(ref DbParameterCollection DbParameters)
        {
            ExecutionResult.Context = "DbObject:BuildSQLUpdate";
            // If mSQLUpdateCache And SQLUpdate.Trim <> "" Then Exit Sub

            int i = 0;
            string SQL1 = "";
            string SQL2 = "";
            StringBuilder s_SQL1 = new StringBuilder();
            StringBuilder s_SQL2 = new StringBuilder();
            DbColumn dbColumn = null;
            //string Prefix = "";
            string parametername = "";
            string dbcolumnname = "";
            string SS = "";
            System.Data.Common.DbParameter p1; // = objFactory.CreateParameter();
            DbParameters.Clear();
            foreach (DbColumn currentDbColumn in mDbColumns)
            {
                dbColumn = currentDbColumn;

                //if (dbColumn.DbType ==  DbType.Time )
                //{
                //    dbColumn.DbType = dbColumn.DbType;
                //}

                SS = dbColumn.ColumnOrdinal.ToString();
                //SS = (dbColumn.ColumnOrdinal + 1).ToString();

                //if (dbColumn.IsIdentity == false)
                if (dbColumn.IsReadOnly == false)
                {
                    parametername = mDbConfig.ParameterNamePrefixE + SS;
                    dbcolumnname = dbColumn.DbColumnName;
                    switch (mDbConfig.Provider)
                    {
                        case Providers.SqlServer:
                            {
                                s_SQL1.Append(dbcolumnname + "=" + parametername + ", ");
                                break;
                            }
                        case Providers.DB2_iSeries :
                            {
                                s_SQL1.Append(dbcolumnname + "=" + parametername + ", ");
                                break;
                            }
                        case Providers.MySQL :
                            {
                                s_SQL1.Append(dbcolumnname + "=" + parametername + ", ");
                                break;
                            }
                        case Providers.OleDb or Providers.OleDb_DB2:
                            {
                                s_SQL1.Append(dbcolumnname + "=?, ");
                                break;
                            }

                        case Providers.ODBC or Providers.ODBC_DB2:
                            {
                                s_SQL1.Append(dbcolumnname + "=?, ");
                                break;
                            }

                        case Providers.OracleDataAccess:
                            {
                                s_SQL1.Append(dbcolumnname + "=" + parametername + ", ");
                                break;
                            }

                        case Providers.ConfigDefined:
                            {
                                s_SQL1.Append(dbcolumnname + "=?, ");
                                break;
                            }
                    }


                    p1 = objFactory.CreateParameter();

                    switch (mDbConfig.Provider)
                    {
                        case Providers.OracleDataAccess:
                            {
                                
                                if (dbColumn.DbType == DbType.Time)
                                {
                                    p1.DbType = DbType.String;
                                }
                                else
                                {
                                    p1.DbType = dbColumn.DbType;
                                }
                                break;
                            }

                        case Providers.SqlServer:

                            System.Data.SqlClient.SqlParameter _p1;
                            _p1  = (System.Data.SqlClient.SqlParameter)p1;
                            
                            if (dbColumn.DbType == DbType.Time)
                            {
                                _p1.SqlDbType = SqlDbType.Time; 
                                
                            }
                            else
                            {
                                _p1.DbType = dbColumn.DbType;
                            }
                            break;

                        default:
                            {
                                p1.DbType =  dbColumn.DbType;
                                break;
                            }
                    }

                    p1.ParameterName = parametername;
                    p1.SourceColumn = dbcolumnname;
                    DbParameters.Add(p1);
                }
            }

            string SS2 = "";
            if (this.GetPrimaryKeyDbColumns().Count > 0)
            {
                System.Data.Common.DbParameter p2 = objFactory.CreateParameter();
                foreach (DbColumn currentDbColumn1 in GetPrimaryKeyDbColumns())
                {
                    dbColumn = currentDbColumn1;
                    SS2 = dbColumn.ColumnOrdinal.ToString();
                    //SS2 = (dbColumn.ColumnOrdinal + 1).ToString();

                    if (dbColumn.IsPrimaryKey)
                    {
                        parametername = mDbConfig.ParameterNamePrefixE + SS2 + "_ShadowValue";
                        dbcolumnname = dbColumn.DbColumnName;
                        switch (mDbConfig.Provider)
                        {
                            case Providers.SqlServer:
                                {
                                    s_SQL2.Append(dbcolumnname + "=" + parametername);
                                    break;
                                }
                            case Providers.DB2_iSeries :
                                {
                                    s_SQL2.Append(dbcolumnname + "=" + parametername);
                                    break;
                                }
                            case Providers.MySQL :
                                {
                                    s_SQL2.Append(dbcolumnname + "=" + parametername);
                                    break;
                                }
                            case Providers.OleDb or Providers.OleDb_DB2:
                                {
                                    s_SQL2.Append(dbcolumnname + "=?");
                                    break;
                                }

                            case Providers.ODBC or Providers.ODBC_DB2:
                                {
                                    s_SQL2.Append(dbcolumnname + "=?");
                                    break;
                                }

                            case Providers.OracleDataAccess:

                                {
                                    s_SQL2.Append(dbcolumnname + "=" + parametername);
                                    break;
                                }

                            case Providers.ConfigDefined:
                                {
                                    s_SQL2.Append(dbcolumnname + "=?");
                                    break;
                                }
                        }

                        s_SQL2.Append(" AND ");
                        p2 = objFactory.CreateParameter();
                        p2.ParameterName = parametername;
                        p2.SourceColumn = dbcolumnname;
                        p2.DbType = dbColumn.DbType;
                        DbParameters.Add(p2);
                    }
                }
            }
            else
            {
                //System.Data.Common .DbParameter p2 = objFactory.CreateParameter();
                foreach (DbColumn currentDbColumn2 in mDbColumns)
                {
                    dbColumn = currentDbColumn2;
                    SS2 = dbColumn.ColumnOrdinal.ToString();
                    //SS2 = (dbColumn.ColumnOrdinal + 1).ToString();
                    parametername = mDbConfig.ParameterNamePrefixE + SS2 + "_ShadowValue";
                    dbcolumnname = dbColumn.DbColumnName;
                    switch (mDbConfig.Provider)
                    {
                        case Providers.SqlServer:
                            {
                                s_SQL2.Append(dbcolumnname + "=" + parametername);
                                break;
                            }

                        case Providers.DB2_iSeries :
                            {
                                s_SQL2.Append(dbcolumnname + "=" + parametername);
                                break;
                            }

                        case Providers.MySQL:
                            {
                                s_SQL2.Append(dbcolumnname + "=" + parametername);
                                break;
                            }

                        case Providers.OleDb or Providers.OleDb_DB2:
                            {
                                s_SQL2.Append(dbcolumnname + "=?");
                                break;
                            }

                        case Providers.ODBC or Providers.ODBC_DB2:
                            {
                                s_SQL2.Append(dbcolumnname + "=?");
                                break;
                            }

                        case Providers.OracleDataAccess:
                            {
                                s_SQL2.Append(dbcolumnname + "=" + parametername);
                                break;
                            }

                        case Providers.ConfigDefined:
                            {
                                s_SQL2.Append(dbcolumnname + "=?");
                                break;
                            }
                    }

                    s_SQL2.Append(" AND ");
                    System.Data.Common.DbParameter p2 = objFactory.CreateParameter();
                    p2.ParameterName = parametername;
                    p2.SourceColumn = dbcolumnname;
                    p2.DbType = dbColumn.DbType;
                    DbParameters.Add(p2);
                }
            }

            SQL1 = s_SQL1.ToString();
            SQL2 = s_SQL2.ToString();
            s_SQL1 = null;
            s_SQL2 = null;
            SQL1 = SQL1.Substring(0, SQL1.Length - 2);
            SQL2 = SQL2.Substring(0, SQL2.Length - 5);
            s_SQL1 = new StringBuilder();
            s_SQL1.Append("UPDATE ");
            s_SQL1.Append(DbTableName);
            s_SQL1.Append(" SET ");
            s_SQL1.Append(SQL1);
            s_SQL1.Append(" WHERE ");
            s_SQL1.Append(SQL2);
            SQLUpdate = s_SQL1.ToString();
        }

        public DbColumn GetDbColumnByDbColumnNameE(string Name)
        {
            ExecutionResult.Context = "DbObject:GetDbColumnByColunNameE";

            if (DbColumns.Contains(Name))
            {
                return DbColumns[Name];
            }
            else
            {
                return null;
            }


            //foreach (DbColumn DbColumn in GetDbColumns())
            //{
            //    if (DbColumn.DbColumnNameE.Trim().ToUpper() == Name.Trim().ToUpper())
            //    {
            //        return DbColumn;
            //    }
            //}
            //return null;
        }

        public DbColumn GetDbColumnByDbColumnName(string Name)
        {
            ExecutionResult.Context = "DbObject:GetDbColumnByColunName";
            foreach (DbColumn DbColumn in GetDbColumns())
            {
                if (DbColumn.DbColumnName.Trim().ToUpper() == Name.Trim().ToUpper())
                {
                    return DbColumn;
                }
            }

            return null;
        }

        public DbColumn GetDbColumn(string Name)
        {
            ExecutionResult.Context = "DbObject:GetDbColumn";
            if (DbColumns == null)
            {
                DbColumns = GetDbColumnsE();
            }
            try
            {
                return DbColumns[Name];
            }
            catch (Exception)
            {
                return null;


            }

            //foreach (DbColumn DbColumn in GetDbColumns())
            //{
            //    if (DbColumn.DbColumnName.Trim().ToUpper() == Name.Trim().ToUpper())
            //    {
            //        return DbColumn;
            //    }
            //}
            //return null;

        }


        public DbColumns GetDbColumns()
        {
            if (DbColumns == null)
            {
                DbColumns = GetDbColumnsE();
            }
            return DbColumns;
        }

     


        public DbColumns GetDbColumnsE()
        {
            ExecutionResult.Context = "DbObject:GetDbColumns";
            FieldInfo xfi;
            DbColumns _DbColumns = new DbColumns();
            DbColumn dbColumn = null;
            if (Fields == null)
            {
                Fields = GetFields(this);
            }

            foreach (var Field in Fields)
            {
                switch (InterfaceMode)
                {
                    case InterfaceModeEnum.Private:
                        {
                            if (Field.Name is object)
                            {
                                xfi = GetType().GetField(Field.Name, BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static);
                                dbColumn = (DbColumn)xfi.GetValue(this);
                            }

                            break;
                        }

                    case InterfaceModeEnum.Public:
                        {
                            if (Field.Name is object)
                            {
                                dbColumn = (DbColumn)Interaction.CallByName(this, Field.Name, CallType.Get);
                            }

                            break;
                        }

                    case InterfaceModeEnum.Property:
                        {
                            if (Field.Name is object)
                            {
                                xfi = GetType().GetField(Field.Name, BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static);
                                dbColumn = (DbColumn)xfi.GetValue(this);
                            }
                            break;
                        }
                }
                dbColumn.Name = Field.Name;
                dbColumn.DbObject = this;
                _DbColumns.Add(dbColumn);
            }
            return _DbColumns;
        }

        public DbParameters GetDbParameters()
        {
            ExecutionResult.Context = "DbObject:GetDbParameters";
            Parameter[] _Parameters = null;
            FieldInfo xfi;
            DbParameters _DbParameters = new DbParameters();
            DbParameter _DbParameter = null;
            if (_Parameters == null)
            {
                _Parameters = GetParameters(this);
            }

            if (_Parameters is null)
                return _DbParameters;

            foreach (Parameter _Parameter in _Parameters)
            {
                switch (InterfaceMode)
                {
                    case InterfaceModeEnum.Private:
                        {
                            if (_Parameter.Name != null)
                            {
                                xfi = GetType().GetField(_Parameter.Name, BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static);
                                _DbParameter = (DbParameter)xfi.GetValue(this);
                                _DbParameter.ParameterName = _Parameter.ParameterName;
                                _DbParameter.DbObject = this;
                                _DbParameter.IDbParameter = CreateDbParameter(_DbParameter.ParameterName, _DbParameter.DbType, _DbParameter.Direction, _DbParameter.Size);
                                _DbParameters.Add(_DbParameter);
                            }

                            break;
                        }

                    case InterfaceModeEnum.Public:
                        {
                            if (_Parameter.Name != null)
                            {
                                _DbParameter = (DbParameter)Interaction.CallByName(this, _Parameter.Name, CallType.Get);
                                _DbParameter.ParameterName = _Parameter.ParameterName;
                                _DbParameter.DbObject = this;
                                _DbParameter.IDbParameter = CreateDbParameter(_DbParameter.ParameterName, _DbParameter.DbType, _DbParameter.Direction, _DbParameter.Size);
                                _DbParameters.Add(_DbParameter);
                            }

                            break;
                        }

                    case InterfaceModeEnum.Property:
                        {
                            if (_Parameter.Name != null)
                            {
                                xfi = GetType().GetField(_Parameter.Name, BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static);
                                _DbParameter = (DbParameter)xfi.GetValue(this);
                                _DbParameter.ParameterName = _Parameter.ParameterName;
                                _DbParameter.DbObject = this;
                                _DbParameter.IDbParameter = CreateDbParameter(_DbParameter.ParameterName, _DbParameter.DbType, _DbParameter.Direction, _DbParameter.Size);
                                _DbParameters.Add(_DbParameter);
                            }

                            break;
                        }
                }
            }

            mDbParameters = _DbParameters;
            return _DbParameters;
        }

        public bool CheckForExistingPKeys(ConnectionState connectionstate)
        {
            ExecutionResult.Context = "DbObject:CheckForExistingPKeys";
            DbColumns PKeys = new DbColumns();

            string SQLWhere = "";
            string SQL = "";
            int i = 0;
            string ParameterName;
            DbCommand lobjcommand;
            lobjcommand = objFactory.CreateCommand();
            lobjcommand.Parameters.Clear();
            PKeys = GetPrimaryKeyDbColumns();
            if (PKeys.Count == 0)
                return false;
            foreach (DbColumn PKCol in PKeys)
            {
                if (PKCol.OriginalValue != PKCol.Value)
                {
                    i++;
                }
            }

            if (i > 0)
            {
                i = 0;
                foreach (DbColumn PKCol in PKeys)
                {
                    ParameterName = mDbConfig.ParameterNamePrefixE + i.ToString();
                    SQLWhere = SQLWhere + PKCol.DbColumnName + "=" + mDbConfig.ParameterNamePrefixE + i + " AND ";
                    DbParameter Parameter = null;
                    int parindex = lobjcommand.Parameters.Add(Parameter);
                    Parameter.ParameterName = ParameterName;
                    Parameter.Value = PKCol.Value;
                    Parameter.Direction = ParameterDirection.Input;
                    lobjcommand.Parameters.Add(Parameter);
                    i++;
                }
            }

            if (string.IsNullOrEmpty(SQLWhere))
                return false;
            if (SQLWhere.Length > 5)
                SQLWhere = SQLWhere.Substring(0, SQLWhere.Length - 4);
            SQL = "SELECT COUNT(*) FROM " + DbTableName + " WHERE " + SQLWhere;
            lobjcommand.CommandText = SQL;
            lobjcommand.CommandType = CommandType.Text;
            lobjcommand.Connection = DbConnection; // objConnection
            object o = null;
            try
            {
                if (DbConnection.State == System.Data.ConnectionState.Closed)
                {
                    DbConnection.Open();
                }
                o = lobjcommand.ExecuteScalar();
            }
            catch (Exception ex)
            {
                HandleExceptions(ex);
            }
            finally
            {
                lobjcommand.Parameters.Clear();
            }

            lobjcommand.Dispose();
            if (Convert.ToInt32(o) != 0d)
            {
                UndoChanges();
                return true;
            }
            else
            {
                return false;
            }
        }

        #region Update Method

        public bool CheckForDataChange()
        {
            bool CheckForDataChangeRet = default;
            ExecutionResult.Context = "DbObject:CheckForDataChange";
            if (mIsReadOnly == true)
                return CheckForDataChangeRet;
            CheckForDataChangeRet = false;
            if (objDataTable is null)
            {
                return false;
            }

            int i = 0;
            DataRow Row = null;
            object Editedvalue = null;
            object DBvalue = null;
            try
            {
                Row = objDataTable.Rows[(int)mCurrentPosition];
            }
            catch
            {
                return false;
            }

            foreach (DbColumn dbcolumn in mDbColumns)
            {
                if (dbcolumn.CheckForDataChange() == true)
                {
                    i = i + 1;
                    break;
                }
            }

            if (i > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public int Update()
        {
            ExecutionResult.Context = "DbObject:Update";
            if (mIsReadOnly == true)
                return default;
            return Update(mDataBinding);
        }

        public int Update(DataBoundControlsBehaviour UpdateBoundControlsBehaviour)
        {
            int AffectedRecords = 0;
            ExecutionResult.Reset();
            ExecutionResult.Context = "DbObject:Update";


            if (mAddNewStatus == true)
            {
                AffectedRecords = Insert(UpdateBoundControlsBehaviour);
                if (AffectedRecords > 0)
                {
                    mDataChanged = false;
                    mAddNewStatus = false;
                }
                else
                {
                    mAddNewStatus = true;
                }

                if (DisableEvents == false)
                {
                    DataEventAfter?.Invoke(DataEventType.Update);
                }

                return AffectedRecords;
            }


            if (UseDataReader == true)
                return AffectedRecords;
            if (mIsReadOnly == true)
                return AffectedRecords;


            DataRow Original_CurrentDataRow = Utilities.DataTableHelper.CloneDataRow(CurrentDataRow);

            DbCommand lobjCommand;
            int i = -1;
            DataRow Row = null;
            if (UpdateBoundControlsBehaviour == DataBoundControlsBehaviour.BasicDALDataBinding & mCurrentPosition < 0)
                return AffectedRecords;

            switch (mDbObjectType)
            {
                case DbObjectTypeEnum.Table or DbObjectTypeEnum.SQLQuery:
                    {
                        break;
                    }

                default:
                    {
                        return AffectedRecords;
                    }
            }

            bool Cancel = false;
            if (DisableEvents == false)
            {
                DataEventBefore?.Invoke(DataEventType.Update, ref Cancel);
            }

            if (Cancel == true)
                return AffectedRecords;

            lobjCommand = objUpdateCommand;
            Row = objDataTable.Rows[(int)mCurrentPosition];
            if (UpdateBoundControlsBehaviour == DataBoundControlsBehaviour.WindowsFormsDataBinding)
            {
                CurrencyManager.EndCurrentEdit();
            }


            // SUPERCRITIC SECTION

            if (UpdateBoundControlsBehaviour != DataBoundControlsBehaviour.NoDataBinding)
            {
                if (UseParallelism == true)
                {
                    System.Threading.Tasks.Parallel.For(0, mDbColumns.Count, _iColumn =>
                    {
                        DbColumn _dbcolumn = mDbColumns.get_Item(_iColumn);
                        UpdateDbColumnsDataBinding(ref Row, ref _dbcolumn, ref lobjCommand, UpdateBoundControlsBehaviour);
                    });
                }
                else
                {
                    for (int _i = 0; _i < mDbColumns.Count; _i++)
                    {
                        DbColumn _dbColumn = mDbColumns.get_Item(_i);
                        UpdateDbColumnsDataBinding(ref Row, ref _dbColumn, ref lobjCommand, UpdateBoundControlsBehaviour);
                        mDbColumns.set_Item(_i, _dbColumn);
                    }

                }
            }
            else
            {
                //NO BOUND CONTROLS ALWAYS GO PARALLEL
                System.Threading.Tasks.Parallel.For(0, mDbColumns.Count, _iColumn =>
                {
                    DbColumn _iDbColumn = mDbColumns.get_Item(_iColumn);
                    UpdateDbColumnsNoDataBinding(ref _iDbColumn, ref lobjCommand);
                });
            }


            if (UpdateBatchEnabled == true)
            {
                CurrentDataRow = objDataTable.Rows[(int)mCurrentPosition];
                // this.UpdateBatch();
                mAddNewStatus = false;
                mRowCount = objDataTable.Rows.Count;
                if (DisableEvents == false)
                {
                    DataEventAfter?.Invoke(DataEventType.Update);
                }
                return 1; //AffectedRecords;
            }


          
                try
                {
                    ExecutionResult.Context = "DbObject:Update.ExecuteNonQuery";
                    lobjCommand.CommandText = SQLUpdate;
                    if (objConnection.State == System.Data.ConnectionState.Closed)
                    {
                        objConnection.Open();
                    }

                    if (lobjCommand.Transaction is null & DbConfig.DbTransaction is object)
                    {
                        switch (mDbConfig.Provider)
                        {
                            case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                                {
                                    lobjCommand.Transaction = DbConfig.DbTransaction;
                                    break;
                                }

                            default:
                                {
                                    lobjCommand.Transaction = DbConfig.DbTransaction;
                                    break;
                                }
                        }
                    }

                if (this.AuditingEnabled ==true)
                {
                  this.DbAuditing.WriteAudit(i.ToString(), AuditOperations.BeforeUpdate);
                }
                
                i = lobjCommand.ExecuteNonQuery();


                if (this.AuditingEnabled == true)
                {
                    //this.DbAuditing.WriteAudit(i.ToString(), AuditOperations.AfterUpdate) ;
                }


            }
            catch (Exception ex)
                {
                    HandleExceptions(ex, "UPDATE" + "\r\n" + DumpUpdateCommandParameters());
                    i = 0;
                }
                finally
                {
                }
          

            mAddNewStatus = false;
            mRowCount = objDataTable.Rows.Count;
            if (DisableEvents == false)
            {
                DataEventAfter?.Invoke(DataEventType.Update);
            }
            if (mDbConfig.DbConnectionKeepOpen == false)
                DbConnection.Close();
            return i;
        }

        private void UpdateDbColumnsDataBinding(ref DataRow Row, ref DbColumn _dbColumn, ref DbCommand lobjCommand, DataBoundControlsBehaviour UpdateBoundControlsBehaviour)
        {
            object value = null;
            object xValue = null;
            object zValue = null;
            object rowvalue = null;

            //if (_dbColumn.Name == "_DbC_recordingtime")
            //{
            //    int a = 0;
            //}

            if (UpdateBoundControlsBehaviour == DataBoundControlsBehaviour.WindowsFormsDataBinding)
            {
                _dbColumn.Value = Row[_dbColumn.DbColumnNameE];
            }

            if (UpdateBoundControlsBehaviour == DataBoundControlsBehaviour.BasicDALDataBinding)
            {
                foreach (BoundControl _BoundControl in _dbColumn.BoundControls)
                {
                    switch (_BoundControl.BindingBehaviour)
                    {
                        case BasicDAL.BindingBehaviour.ReadWrite or BasicDAL.BindingBehaviour.ReadWriteAddNew:
                            {
                                //value = Interaction.CallByName(_BoundControl.Control, _BoundControl.PropertyName, CallType.Get);
                                value = Interaction.CallByName(_BoundControl.Control, _BoundControl.PropertyName, CallType.Get);
                                try
                                {
                                    _dbColumn.Value = Utilities.DbValueCast(value, _dbColumn.DbType, NullDateValue, _dbColumn.DateTimeResolution);
                                    rowvalue = _dbColumn.Value;
                                    switch (mDbConfig.Provider)
                                    {
                                        case Providers.OracleDataAccess:
                                            {

                                                Row[_dbColumn.DbColumnName.Replace("\"", String.Empty)] = rowvalue;
                                                break;
                                            }
                                        default:
                                            {
                                                Row[_dbColumn.DbColumnNameE] = rowvalue;
                                                break;
                                            }
                                    }

                                    _dbColumn.DataChanged = false;
                                }
                                catch (Exception ex)
                                {

                                    // MsgBox("ORROR " & dbcolumn.DbColumnNameE & " - " & ex.Message)

                                }
                                break;
                            }

                        default:
                            {
                                break;
                            }
                    }
                }
            }



            if (UpdateBatchEnabled == false)
            {
                // controlli su valorei NULL dell colonna
                if (_dbColumn.Value is null)
                {
                    xValue = DBNull.Value;
                }
                else
                {
                    xValue = _dbColumn.Value;
                }

                if (_dbColumn.OriginalValue is null)
                {
                    zValue = DBNull.Value;
                }
                else
                {
                    zValue = _dbColumn.OriginalValue;
                }

                string ParameterName = mDbConfig.ParameterNamePrefixE + _dbColumn.ColumnOrdinal.ToString();
                try
                {
                    if (_dbColumn.IsReadOnly == false)
                    {
                        lobjCommand.Parameters[ParameterName].Value = ConvertParameterValue(_dbColumn, xValue);
                    }
                }
                catch
                {
                }

                try
                {
                    if (_dbColumn.IsPrimaryKey)
                    {
                        lobjCommand.Parameters[ParameterName + "_ShadowValue"].Value = ConvertParameterValue(_dbColumn, zValue);
                    }
                }
                catch
                {
                }
            }
        }

        private void UpdateDbColumnsNoDataBinding(ref DbColumn _idbColumn, ref DbCommand lobjCommand)
        {
            object _xValue;
            object _zValue;
            // controlli su valorei NULL dell colonna
            if (_idbColumn.Value is null)
            {
                _xValue = DBNull.Value;
            }
            else
            {
                _xValue = _idbColumn.Value;
            }

            if (_idbColumn.OriginalValue is null)
            {
                _zValue = DBNull.Value;
            }
            else
            {
                _zValue = _idbColumn.OriginalValue;
            }

            if (_idbColumn.IsReadOnly == false)
            {
                try
                {
                    //lobjCommand.Parameters[mDbConfig.ParameterNamePrefixE + _idbColumn.ColumnOrdinal.ToString()].Value = ConvertParameterValue(_idbColumn, _xValue);
                    lobjCommand.Parameters[_idbColumn.ColumnOrdinal].Value = ConvertParameterValue(_idbColumn, _xValue);
                }
                catch
                {
                    // HandleExceptions(ex, "UPDATE" & vbCrLf & DumpUpdateCommandParameters())
                }
            }

            try
            {
                if (_idbColumn.IsPrimaryKey)
                {
                    lobjCommand.Parameters[mDbConfig.ParameterNamePrefixE + _idbColumn.ColumnOrdinal.ToString() + "_ShadowValue"].Value = ConvertParameterValue(_idbColumn, _zValue);
                }
            }
            catch
            {
                // HandleExceptions(ex, "UPDATE" & vbCrLf & DumpUpdateCommandParameters())
            }
            // Else
            // End If

        }

        public string DumpUpdateCommandParameters()
        {
            ExecutionResult.Context = "DbObject:DumpUpdateCommandParametes";
            string vbCrLf = "\r\n";
            string s = "";
            StringBuilder Dump = new StringBuilder();
            Dump.Append("DBObject " + ToString() + ".UpdateCommandParameters - DBTableName = " + DbTableName + vbCrLf);
            Dump.Append("------------------------------------------------------------------------------" + vbCrLf);
            int i = 0;
            foreach (System.Data.Common.DbParameter Par in objUpdateCommand.Parameters)
            {
                try
                {
                    Dump.Append(s + Par.ParameterName + "(" + Par.SourceColumn + ", " + Par.DbType.ToString() + ") = " + Convert.ToString(Par.Value) + vbCrLf);
                }
                catch
                {
                }
            }

            Dump.Append("------------------------------------------------------------------------------" + vbCrLf);
            return Dump.ToString();
        }

        public string DumpCommandParameters()
        {
            ExecutionResult.Context = "DbObject:DumpCommandParameters";
            string s = "";
            string vbCrLf = "\r\n";
            StringBuilder Dump = new StringBuilder();
            Dump.Append("DBObject " + ToString() + ".CommandParameters - DBTableName = " + DbTableName + vbCrLf);
            Dump.Append("------------------------------------------------------------------------------" + vbCrLf);
            int i = 0;
            foreach (System.Data.Common.DbParameter Par in objCommand.Parameters)
            {
                try
                {
                    Dump.Append(s + Par.ParameterName + "(" + Par.SourceColumn + ", " + Par.DbType.ToString() + ") = " + Par.Value.ToString() + vbCrLf);
                }
                catch
                {
                }
            }

            Dump.Append("------------------------------------------------------------------------------" + vbCrLf);
            return Dump.ToString();
        }

        public string DumpInsertCommandParameters()
        {
            ExecutionResult.Context = "DbObject:DumpInsertCommandParameters";
            string s = "";
            string vbCrLf = "\r\n";
            StringBuilder Dump = new StringBuilder();
            Dump.Append("DBObject " + ToString() + ".InsertCommandParameters - DBTableName = " + DbTableName + vbCrLf);
            Dump.Append("------------------------------------------------------------------------------" + vbCrLf);
            int i = 0;
            foreach (System.Data.Common.DbParameter Par in objInsertCommand.Parameters)
            {
                try
                {
                    Dump.Append(s + Par.ParameterName + "(" + Par.SourceColumn + ", " + Par.DbType.ToString() + ") = " + Par.Value.ToString() + vbCrLf);
                }
                catch
                {
                }
            }

            Dump.Append("------------------------------------------------------------------------------" + vbCrLf);
            return Dump.ToString();
        }

        public string DumpStoredProcedureCommandParameters()
        {
            ExecutionResult.Context = "DbObject:DumpStoredProcedureCommandParameter";
            string s = "";
            string vbCrLf = "\r\n";
            StringBuilder Dump = new StringBuilder();
            Dump.Append("DbStoredProcedure " + ToString() + ".Parameters");
            Dump.Append("------------------------------------------------------------------------------" + vbCrLf);
            int i = 0;
            foreach (System.Data.Common.DbParameter Par in objStoredProcedureCommand.Parameters)
            {
                try
                {
                    Dump.Append(s + Par.ParameterName + "(" + Par.SourceColumn + ", " + Par.DbType.ToString() + ") = " + Par.Value.ToString() + vbCrLf);
                }
                catch
                {
                }
            }

            Dump.Append("------------------------------------------------------------------------------" + vbCrLf);
            return Dump.ToString();
        }

        public string DumpDeleteCommandParameters()
        {
            ExecutionResult.Context = "DbObject:DumpDeleteCommandParameters";
            string s = "";
            string vbCrLf = "\r\n";
            StringBuilder Dump = new StringBuilder();
            Dump.Append("DBObject " + ToString() + ".DeleteCommandParameters - DBTableName = " + DbTableName + vbCrLf);
            Dump.Append("------------------------------------------------------------------------------" + vbCrLf);
            int i = 0;
            foreach (System.Data.Common.DbParameter Par in objDeleteCommand.Parameters)
            {
                try
                {
                    Dump.Append(s + Par.ParameterName + "(" + Par.SourceColumn + ", " + Par.DbType.ToString() + ") = " + Par.Value.ToString() + vbCrLf);
                }
                catch
                {
                }
            }

            Dump.Append("------------------------------------------------------------------------------" + vbCrLf);
            return Dump.ToString();
        }

        public string DumpExecuteCommandParameters()
        {
            ExecutionResult.Context = "DbObject:DumpExecuteCommandParameters";
            string s = "";
            string vbCrLf = "\r\n";
            StringBuilder Dump = new StringBuilder();
            Dump.Append("DBObject " + ToString() + ".ExecuteCommandParameters - DBTableName = " + DbTableName + vbCrLf);
            Dump.Append("------------------------------------------------------------------------------" + vbCrLf);
            int i = 0;
            foreach (System.Data.Common.DbParameter Par in objExecuteCommand.Parameters)
            {
                try
                {
                    Dump.Append(s + Par.ParameterName + "(" + Par.SourceColumn + ", " + Par.DbType.ToString() + ") = " + Par.Value.ToString() + vbCrLf);
                }
                catch
                {
                }
            }

            Dump.Append("------------------------------------------------------------------------------" + vbCrLf);
            return Dump.ToString();
        }
        #endregion



        #region Cast Function
        private object Cast(object Value, DbType dbType, DateTimeResolution DateTimeResolution)
        {
            object CastRet = default;
            ExecutionResult.Context = "DbObject:Cast";
            // If Value Is Nothing Or Value = "" Then Value = System.DBNull.Value


            try
            {
                switch (dbType)
                {
                    case DbType.Object:
                        {
                            CastRet = Value;
                            break;
                        }

                    case DbType.AnsiString or DbType.AnsiStringFixedLength or DbType.String or DbType.StringFixedLength:
                        {
                            CastRet = Convert.ToString(Value);
                            break;
                        }

                    case DbType.Byte:
                        {
                            CastRet = Convert.ToByte(Value);
                            break;
                        }

                    case DbType.Boolean:
                        {
                            CastRet = Convert.ToBoolean(Value);
                            break;
                        }

                    case DbType.Currency or DbType.Decimal or DbType.VarNumeric:
                        {
                            CastRet = Convert.ToDecimal(Value);
                            break;
                        }

                    case DbType.Single:
                        {
                            CastRet = Convert.ToSingle(Value);
                            break;
                        }

                    case DbType.Date or DbType.DateTime or DbType.DateTime2 or DbType.Time:
                        {
                            if (Value is null == false)
                            {
                                CastRet = Utilities.TruncateDateTime(Convert.ToDateTime(Value), DateTimeResolution);
                            }
                            else
                            {
                                CastRet = DBNull.Value;
                            }

                            break;
                        }

                    case DbType.Double:
                        {
                            CastRet = Convert.ToDouble(Value);
                            break;
                        }

                    case DbType.Guid:
                        {
                            CastRet = Convert.ToString(Value);
                            break;
                        }

                    case DbType.SByte:
                        {
                            CastRet = Convert.ToSByte(Value);
                            break;
                        }

                    case DbType.Int16:
                        {
                            CastRet = Convert.ToInt16(Value);
                            break;
                        }

                    case DbType.Int32:
                        {
                            CastRet = Convert.ToInt32(Value);
                            break;
                        }

                    case DbType.Int64:
                        {
                            CastRet = Convert.ToInt64(Value);
                            break;
                        }

                    case DbType.UInt16:
                        {
                            CastRet = Convert.ToUInt16(Value);
                            break;
                        }

                    case DbType.UInt32:
                        {
                            CastRet = Convert.ToUInt32(Value);
                            break;
                        }

                    case DbType.UInt64:
                        {
                            CastRet = Convert.ToUInt64(Value);
                            break;
                        }

                    default:
                        {
                            CastRet = Value;
                            break;
                        }
                }
            }
            catch
            {
                return null;
            }

            return CastRet;
        }

        private Type DbTypeToDataType(DbType dbType)
        {
            Type DbTypeToDataTypeRet = default;
            ExecutionResult.Context = "DbObject:DbTypeToDataType";
            Type cast;
            try
            {
                switch (dbType)
                {
                    case DbType.AnsiString or DbType.AnsiStringFixedLength or DbType.String or DbType.StringFixedLength:
                        {
                            cast = Type.GetType("System.String");
                            break;
                        }

                    case DbType.Byte:
                        {
                            cast = Type.GetType("System.Byte");
                            break;
                        }

                    case DbType.Boolean:
                        {
                            cast = Type.GetType("System.Boolean");
                            break;
                        }

                    case DbType.Currency or DbType.Decimal or DbType.VarNumeric:
                        {
                            cast = Type.GetType("System.Decimal");
                            break;
                        }

                    case DbType.Single:
                        {
                            cast = Type.GetType("System.Single");
                            break;
                        }

                    case DbType.Date or DbType.DateTime:
                        {
                            cast = Type.GetType("System.Datetime");
                            break;
                        }

                    case DbType.Time:
                        {
                            cast = Type.GetType("System.TimeSpan");
                            break;
                        }

                    case DbType.Double:
                        {
                            cast = Type.GetType("System.Double");
                            break;
                        }

                    case DbType.Guid:
                        {
                            cast = Type.GetType("System.Guid");
                            break;
                        }

                    case DbType.SByte:
                        {
                            cast = Type.GetType("System.SByte");
                            break;
                        }

                    case DbType.Int16:
                        {
                            cast = Type.GetType("System.Int16");
                            break;
                        }

                    case DbType.Int32:
                        {
                            cast = Type.GetType("System.Int32");
                            break;
                        }

                    case DbType.Int64:
                        {
                            cast = Type.GetType("System.Int64");
                            break;
                        }

                    case DbType.UInt16:
                        {
                            cast = Type.GetType("System.UInt16");
                            break;
                        }

                    case DbType.UInt32:
                        {
                            cast = Type.GetType("System.UInt32");
                            break;
                        }

                    case DbType.UInt64:
                        {
                            cast = Type.GetType("System.UIint64");
                            break;
                        }

                    default:
                        {
                            cast = Type.GetType("System.String");
                            break;
                        }
                }
            }
            catch
            {
                cast = Type.GetType("System.String");
            }

            DbTypeToDataTypeRet = cast;
            return DbTypeToDataTypeRet;
        }
        #endregion


        public DataRowCollection GetRows(DataSet DataSet)
        {
            DataRowCollection GetRowsRet = default;
            ExecutionResult.Context = "DbObject:GetRows";
            try
            {
                GetRowsRet = DataSet.Tables[0].Rows;
            }
            catch (Exception ex)
            {
                GetRowsRet = null;
                HandleExceptions(ex);
            }

            return GetRowsRet;
        }

        public DataTable GetTable(DataSet DataSet)
        {
            DataTable GetTableRet = default;
            ExecutionResult.Context = "DbObject:GetTable";
            try
            {
                GetTableRet = DataSet.Tables[0];
            }
            catch (Exception ex)
            {
                GetTableRet = null;
                HandleExceptions(ex);
            }

            return GetTableRet;
        }

        public void DisableBoundControls()
        {
            ExecutionResult.Context = "DbObject:DisableBoundControls";
            string ControlName = "";
            string PropertyName = "";
            string ColumnName = "";
            int i = 0;
            bool CanBound = false;
            if (mDataBinding == DataBoundControlsBehaviour.NoDataBinding)
                return;

            foreach (DbColumn dbColumn in mDbColumns)
            {
                foreach (BoundControl BoundControl in dbColumn.BoundControls)
                {
                    try
                    {
                        Interaction.CallByName(BoundControl.Control, "Enabled", CallType.Set, false);
                    }
                    catch
                    {
                    }
                }
            }
        }

        public void ClearData(bool PreserveOriginalValue = false)
        {
            ExecutionResult.Context = "DbObject:ClearData";
            BlankDbColumns(mDataBinding, PreserveOriginalValue);
        }

        public void ClearAllBoundControls()
        {
            ExecutionResult.Context = "DbObject:ClearAllBoundControls";
            if (mDataBinding == DataBoundControlsBehaviour.NoDataBinding)
                return;
            if (UseParallelism == true)
            {
                System.Threading.Tasks.Parallel.For(0, mDbColumns.Count, _iColumn =>
                {
                    DbColumn dbColumn = mDbColumns.get_Item(_iColumn);// (DbColumn)mDbColumns.ElementAtOrDefault(_iColumn);
                    dbColumn.BoundControls.Clear();
                });
            }
            else
            {
                foreach (DbColumn dbColumn in mDbColumns)
                    dbColumn.BoundControls.Clear();
            }
        }

        public void EnableBoundControls()
        {
            ExecutionResult.Context = "DbObject:EnableBoundControls";
            if (mDataBinding == DataBoundControlsBehaviour.NoDataBinding)
                return;

            foreach (DbColumn dbColumn in mDbColumns)
            {
                foreach (BoundControl BoundControl in dbColumn.BoundControls)
                {
                    try
                    {
                        Interaction.CallByName(BoundControl.Control, "enabled", CallType.Set, true);
                    }
                    catch
                    {
                    }
                }
            }
        }

        public void BlankDbColumns(DataBoundControlsBehaviour mDataBinding, bool PreserveShadowValue = false)
        {
            ExecutionResult.Context = "DbObject:BlankDbColumns";

            // PARALLELIZZABILE

            foreach (DbColumn dbColumn in mDbColumns)
            {
                bool CanBound = false;
                dbColumn.Value = null;
                if (PreserveShadowValue == false)
                {
                    dbColumn.OriginalValue = dbColumn.Value;
                }

                if (mDataBinding == DataBoundControlsBehaviour.BasicDALDataBinding)
                {
                    foreach (BoundControl BoundControl in dbColumn.BoundControls)
                    {
                        CanBound = false;
                        switch (BoundControl.BindingBehaviour)
                        {
                            case BasicDAL.BindingBehaviour.ReadWrite or BasicDAL.BindingBehaviour.Write:
                                {
                                    CanBound = true;
                                    break;
                                }

                            case BasicDAL.BindingBehaviour.ReadWriteAddNew or BasicDAL.BindingBehaviour.WriteAddNew:
                                {
                                    if (BindingBehaviour == true)
                                    {
                                        CanBound = true;
                                    }

                                    break;
                                }

                            default:
                                {
                                    break;
                                }
                        }

                        if (CanBound)
                        {
                            try
                            {
                                Interaction.CallByName(BoundControl.Control, BoundControl.PropertyName, CallType.Set, null);
                            }
                            catch
                            {
                            }
                        }
                    }
                }
            }
        }


        #region DataRowGetValues Method
        public void DataRowGetValues(DataRow DataRow)
        {
            ExecutionResult.Context = "DbObject:DataRowGetValues";
            DataRowGetValues(DataRow, DataBoundControlsBehaviour.NoDataBinding);
        }

        public void DataRowGetValues(DataRow DataRow, DataBoundControlsBehaviour mDataBinding = DataBoundControlsBehaviour.NoDataBinding)
        {
            ExecutionResult.Context = "DbObject:DataRowGetValues";
            bool isDataRowValid = false;
            bool Cancel = false;
            if (DisableEvents == false)
            {
                DataEventBefore?.Invoke(DataEventType.Binding, ref Cancel);
            }

            if (Cancel == true)
                return;
            if (DataRow == null)
            {
                BlankDbColumns(mDataBinding);
                isDataRowValid = false;
                CurrentDataRow = null;
                if (DisableEvents == false)
                {
                    DataEventAfter?.Invoke(DataEventType.Binding);
                    if (this.mDataBinding == DataBoundControlsBehaviour.BasicDALDataBinding)
                    {
                        BoundCompleted?.Invoke();
                        DataEventAfter?.Invoke(DataEventType.ControlsBinding);
                    }
                }

                return;
            }
            else
            {
                isDataRowValid = true;
                CurrentDataRow = DataRow;
            }


            // Dim t As DateTime = Now
            // USA SEMPRE CODICE PARALLELO 
            System.Threading.Tasks.Parallel.For(0, mDbColumns.Count, _iColumn =>
                {
                    DbColumn _iDbColumn = mDbColumns.get_Item(_iColumn);
                    DataRowGetDbColumnValue(ref _iDbColumn, ref DataRow, isDataRowValid);
                });
            if (this.mDataBinding == DataBoundControlsBehaviour.BasicDALDataBinding)
            {
                Cancel = false;
                if (DisableEvents == false)
                {
                    DataEventBefore?.Invoke(DataEventType.Binding, ref Cancel);
                }

                if (Cancel == true)
                    return;
                // PARALLELIZZABILE

                InitBoundControls();
                if (UseParallelism == true)
                {
                    System.Threading.Tasks.Parallel.For(0, mDbColumns.Count, _iColumn =>
                    {
                        DbColumn _iDbColumn = mDbColumns.get_Item(_iColumn);
                        DataRowGetDbColumnBoundControls(ref _iDbColumn);
                    });
                }
                else
                {

                    // VECCHIO CODICE SEQUENZIALE

                    for (int _i = 0; _i < mDbColumns.Count; _i++)
                    {
                        DbColumn _dbColumn = mDbColumns.get_Item(_i);
                        DataRowGetDbColumnBoundControls(ref _dbColumn);
                        mDbColumns.set_Item(_i, _dbColumn);
                    }

                }
            }
            // Dim T1 As DateTime = Now
            // Dim elapsed_time As TimeSpan
            // elapsed_time = T1.Subtract(t)
            // Debug.WriteLine(elapsed_time.TotalMilliseconds.ToString())


            mDataChanged = false;
            if (DisableEvents == false)
            {
                DataEventAfter?.Invoke(DataEventType.Binding);
                if (this.mDataBinding == DataBoundControlsBehaviour.BasicDALDataBinding)
                {
                    BoundCompleted?.Invoke();
                    DataEventAfter?.Invoke(DataEventType.ControlsBinding);
                }
            }
        }

        private void DataRowGetDbColumnBoundControls(ref DbColumn _iDbColumn)
        {
            bool CanBound;
            foreach (BoundControl _BoundControl in _iDbColumn.BoundControls)
            {
                CanBound = true;
                // CanBound = False
                // Select Case BoundControl.BindingBehaviour
                // Case Is = BasicDAL.BindingBehaviour.ReadWrite, BasicDAL.BindingBehaviour.Write
                // CanBound = True
                // Case Is = BasicDAL.BindingBehaviour.ReadWriteAddNew, BasicDAL.BindingBehaviour.WriteAddNew
                // If Me.BindingBehaviour = True Then
                // CanBound = True
                // End If
                // Case Else
                // End Select

                //if (_iDbColumn .Name=="_DbC_recordtime")
                //{
                //    int a = 0;
                //}
                if (CanBound)
                {
                    _BoundControl.IsEdited = false;
                    try
                    {
                        if (_iDbColumn.Value is DBNull == false)
                        {
                            switch (_iDbColumn.DbType)
                            {
                                case DbType.Time:

                                    if (Utilities.IsTime(_iDbColumn.Value) == true)
                                    {
                                        Interaction.CallByName(_BoundControl.Control, _BoundControl.PropertyName, CallType.Set, _iDbColumn.Value.ToString());
                                    }
                                    else
                                    {
                                        Interaction.CallByName(_BoundControl.Control, _BoundControl.PropertyName, CallType.Set, null);
                                    }
                                    break;

                                case DbType.Date or DbType.DateTime or DbType.DateTime2 or DbType.DateTimeOffset:
                                    {

                                        if (Utilities.IsDate(_iDbColumn.Value) == true)
                                        {
                                            Interaction.CallByName(_BoundControl.Control, _BoundControl.PropertyName, CallType.Set, _iDbColumn.Value);
                                        }
                                        else
                                        {
                                            Interaction.CallByName(_BoundControl.Control, _BoundControl.PropertyName, CallType.Set, mNullDateValue);
                                        }

                                        break;
                                    }

                                case DbType.Decimal or DbType.Single or DbType.VarNumeric or DbType.Double or DbType.Currency:
                                    {
                                        object _value = Convert.ToString(_iDbColumn.Value, System.Globalization.CultureInfo.InvariantCulture);
                                        Interaction.CallByName(_BoundControl.Control, _BoundControl.PropertyName, CallType.Set, _value);
                                        break;
                                    }

                                default:
                                    {
                                        Interaction.CallByName(_BoundControl.Control, _BoundControl.PropertyName, CallType.Set, _iDbColumn.Value);
                                        break;
                                    }
                            }
                        }
                        else
                        {
                            switch (_iDbColumn.DbType)
                            {
                                case DbType.Date or DbType.Time or DbType.DateTime or DbType.DateTime2 or DbType.DateTimeOffset:

                                    {
                                        Interaction.CallByName(_BoundControl.Control, _BoundControl.PropertyName, CallType.Set, (object)NullDateValue);
                                        break;
                                    }

                                default:
                                    {
                                        Interaction.CallByName(_BoundControl.Control, _BoundControl.PropertyName, CallType.Set, "");
                                        break;
                                    }
                            }
                        }
                    }
                    catch
                    {
                    }
                }
            }
        }

        private void DataRowGetDbColumnValue(ref DbColumn _iDbColumn, ref DataRow DataRow, bool IsDataRowValid)
        {
            if (IsDataRowValid)
            {
                if (ReferenceEquals(DataRow[_iDbColumn.DbColumnNameE], DBNull.Value))
                {
                    _iDbColumn.ValueNoUpdateCurrentDataRow = null;
                }
                else
                {
                    _iDbColumn.ValueNoUpdateCurrentDataRow = DataRow[_iDbColumn.DbColumnNameE];
                }
            }
            else
            {
                _iDbColumn.ValueNoUpdateCurrentDataRow = null;
            }

            _iDbColumn.OriginalValue = _iDbColumn.Value;
            _iDbColumn.DataChanged = false;
        }

        public void UpdateBoundControls(DbColumn DBColumn)
        {
            ExecutionResult.Context = "DbObject:UpdateBoundControls";

            int i = 0;
            bool CanBound = false;
            if (DBColumn == null)
            {
                return;
            }

            DBColumn.OriginalValue = DBColumn.Value;
            DBColumn.DataChanged = false;
            try
            {
                objDataTable.Rows[(int)mCurrentPosition][DBColumn.DbColumnName] = DBColumn.Value;
            }
            catch
            {
            }

            if (mDataBinding == DataBoundControlsBehaviour.BasicDALDataBinding)
            {

                // PARELLELIZZABILE

                foreach (BoundControl BoundControl in DBColumn.BoundControls)
                {
                    CanBound = true;
                    // Select Case BoundControl.BindingBehaviour
                    // Case Is = BasicDAL.BindingBehaviour.ReadWrite, BasicDAL.BindingBehaviour.Write, BasicDAL.BindingBehaviour.ReadAddNew
                    // CanBound = True
                    // Case Is = BasicDAL.BindingBehaviour.ReadWriteAddNew, BasicDAL.BindingBehaviour.WriteAddNew
                    // If Me.BindingBehaviour = True Then
                    // CanBound = True
                    // End If
                    // Case Else
                    // End Select

                    if (CanBound)
                    {
                        try
                        {
                            switch (DBColumn.DbType)
                            {
                                case DbType.Time:
                                    {
                                        //Utilities.CallByName(BoundControl.Control, BoundControl.PropertyName, Utilities.CallType.Set, DBColumn.Value.ToString());
                                        Interaction.CallByName(BoundControl.Control, BoundControl.PropertyName, CallType.Set, DBColumn.Value.ToString());
                                        break;
                                    }

                                default:
                                    {
                                        //Utilities.CallByName(BoundControl.Control, BoundControl.PropertyName, Utilities.CallType.Set, DBColumn.Value);
                                        Interaction.CallByName(BoundControl.Control, BoundControl.PropertyName, CallType.Set, DBColumn.Value);
                                        break;
                                    }
                            }
                        }
                        catch
                        {
                        }
                    }
                }
            }
        }

        public void ResetQBEAttributes()
        {
            ExecutionResult.Context = "DbObject:ResetQBEAttributes";
            if (mDbColumns is null)
            {
                mDbColumns = GetDbColumns();
            }

            foreach (DbColumn dbcolumn in mDbColumns)
            {
                dbcolumn.UseInQBE = false;
                dbcolumn.DisplayInQBEResult = false;
                dbcolumn.QBEResulGridColumnNumber = -1;
            }
        }

        #endregion

        #region Init Method

        private void Init(string connectionstring, Providers provider)
        {
            ExecutionResult.Context = "DbObject:Init(ConnectionString,Provider)";

            FiltersGroup.Clear();
            UseParallelism = DbConfig.UseParallelism;
            mSuppressErrorsNotification = DbConfig.SuppressErrorsNotification;
            mRedirectErrorsNotificationTo = DbConfig.RedirectErrorsNotificationTo;
            mOnlyEntityInitialization = DbConfig.OnlyEntityInitialization;
            mNullDateValue = DbConfig.NullDateValue;
            RuntimeUI = DbConfig.RuntimeUI;
            ValidatorLabelControl = DbConfig.ValidatorLabelControl;


            try
            {
                ResetError();
                mDbObjectInitialized = false;
                mConnectionString = connectionstring;

                    if (DbConnection is null)
                    {
                        objConnection = objFactory.CreateConnection();
                        objConnection.ConnectionString = mConnectionString;
                        objConnection.Open();
                    }
                

                objCommand = objFactory.CreateCommand();
                objCommand.Connection = objConnection;
                objCommand.CommandTimeout = CommandTimeOut;
                objExecuteCommand = objFactory.CreateCommand();
                objExecuteCommand.Connection = objConnection;
                objExecuteCommand.CommandTimeout = CommandTimeOut;
                objInsertCommand = objFactory.CreateCommand();
                objInsertCommand.Connection = objConnection;
                objInsertCommand.CommandTimeout = CommandTimeOut;
                objUpdateCommand = objFactory.CreateCommand();
                objUpdateCommand.Connection = objConnection;
                objUpdateCommand.CommandTimeout = CommandTimeOut;
                objStoredProcedureCommand = objFactory.CreateCommand();
                objStoredProcedureCommand.Connection = objConnection;
                objStoredProcedureCommand.CommandTimeout = CommandTimeOut;
                objDeleteCommand = objFactory.CreateCommand();
                objDeleteCommand.Connection = objConnection;
                objDeleteCommand.CommandTimeout = CommandTimeOut;
                objAdapter = objFactory.CreateDataAdapter();



                mDbColumns = GetDbColumns();


                if (mOnlyEntityInitialization == false)
                {
                    GetDbSchema(ConnectionState.CloseOnExit);
                    if (mLastErrorCode > 0)
                        return;
                }
                else
                {
                    for (int cOrd = 0, loopTo = mDbColumns.Count - 1; cOrd <= loopTo; cOrd++)
                        mDbColumns.get_Item(cOrd).ColumnOrdinal = cOrd;
                }

                mDbParameters = GetDbParameters();

                objCommand.CommandText = GetSQLSelectCommandTextForObjectSchema();
                objAdapter.SelectCommand = objCommand;
                objCommandBuilder = objFactory.CreateCommandBuilder();
                objCommandBuilder.DataAdapter = objAdapter;
                objCommandBuilder.RefreshSchema();
                var argAdapter = objAdapter;
                objCommandBuilder = SetupCommandBuilder(argAdapter);
                objAdapter = argAdapter;
                switch (provider)
                {
                    
                    default:
                        {
                            if (string.IsNullOrEmpty(DbTableName))
                                DbTableName = "[Table0]";
                            break;
                        }
                }
                
                switch (mDbObjectType)
                {
                    case DbObjectTypeEnum.StoredProcedure:
                    case DbObjectTypeEnum.TableFunction:
                    case DbObjectTypeEnum.ScalarFunction:
                        {
                            objStoredProcedureCommand.Parameters.Clear();
                            foreach (DbParameter param in mDbParameters)
                                objStoredProcedureCommand.Parameters.Add(param.IDbParameter);
                            break;
                        }
                    case DbObjectTypeEnum.View:
                        Open(false);
                        break;

                    default:
                        {
                            if (mIsReadOnly == false)
                            {
                                var argDBParameters = objInsertCommand.Parameters;
                                BuildSQLInsert(ref argDBParameters);
                                var argDBParameters1 = objUpdateCommand.Parameters;
                                BuildSQLUpdate(ref argDBParameters1);
                                var argDBParameters2 = objDeleteCommand.Parameters;
                                BuildSQLDelete(ref argDBParameters2);

                                
                                //SetupUpdateCommand(ref objUpdateCommand);
                                //SetupDeleteCommand(ref objDeleteCommand);
                                //SetupInsertCommand(ref objInsertCommand);


                            }

                            Open(false);
                            break;
                        }
                }

                if (ErrorState == false)
                {
                    mDbObjectInitialized = true;
                }
            }
            catch (Exception ex)
            {
                HandleExceptions(ex, ExecutionResult.Context);
                mErrorState = true;
                mLastError = ex.Message;
                mLasteException = ex;
                mLastErrorCode = 1;
            }

           // this.DbAuditing.SetDbObject(this);
        }

        public void SetupInsertCommand(ref DbCommand DbCommand)
        {
            objCommandBuilder.RefreshSchema();
            //DbDataAdapter argAdapter = objAdapter;
            //objCommandBuilder = SetupCommandBuilder(argAdapter);
            //objAdapter = argAdapter;
            //this.InsertCommand = objCommandBuilder.GetInsertCommand();
            switch (this.mDbConfig.Provider)
            {
                case Providers.SqlServer:
                    this.InsertCommand = objCommandBuilder.GetInsertCommand();
                    break;
                case Providers.OleDb:
                    break;
                case Providers.OracleDataAccess:
                    this.InsertCommand = objCommandBuilder.GetInsertCommand();
                    break;
                case Providers.ODBC:
                    this.InsertCommand = objCommandBuilder.GetInsertCommand();
                    break;
                case Providers.ConfigDefined:
                    break;
                case Providers.ODBC_DB2:
                    this.InsertCommand = objCommandBuilder.GetInsertCommand();
                    break;
                case Providers.OleDb_DB2:
                    //this.InsertCommand = objCommandBuilder.GetInsertCommand();
                    break;
                case Providers.DB2_iSeries:
                    //this.InsertCommand = objCommandBuilder.GetInsertCommand();
                    break;
                case Providers.MySQL:
                    break;
                default:
                    break;
            }

        }

        public void SetupUpdateCommand(ref DbCommand DbCommand)
        {
            this.ExecutionResult.Context = "DbObject.SetupUpdateCommand";
            objCommandBuilder.RefreshSchema();
            //DbDataAdapter argAdapter = objAdapter;
            //objCommandBuilder = SetupCommandBuilder(argAdapter);
            //objAdapter = argAdapter;


            switch (this.mDbConfig .Provider)
            {
                case Providers.SqlServer:
                    this.UpdateCommand = objCommandBuilder.GetUpdateCommand();
                    
                    break;
                case Providers.OleDb:
                    break;
                case Providers.OracleDataAccess:
                    this.UpdateCommand = objCommandBuilder.GetUpdateCommand();
                    break;
                case Providers.ODBC:
                    this.UpdateCommand = objCommandBuilder.GetUpdateCommand();
                    break;
                case Providers.ConfigDefined:
                    break;
                case Providers.ODBC_DB2:
                    this.UpdateCommand = objCommandBuilder.GetUpdateCommand();
                    break;
                case Providers.OleDb_DB2:
                    this.UpdateCommand = objCommandBuilder.GetUpdateCommand();
                    break;
                case Providers.DB2_iSeries:
                    //this.UpdateCommand = objCommandBuilder.GetUpdateCommand();
                    break;
                case Providers.MySQL :
                    //this.UpdateCommand = objCommandBuilder.GetUpdateCommand();
                    break;
                default:
                    break;
            }


           


        }

        public void SetupDeleteCommand(ref DbCommand DbCommand)
        {
            objCommandBuilder.RefreshSchema();
            //DbDataAdapter argAdapter = objAdapter;
            //objCommandBuilder = SetupCommandBuilder(argAdapter);
            //objAdapter = argAdapter;
            //this.DeleteCommand = objCommandBuilder.GetDeleteCommand ();
            switch (this.mDbConfig.Provider)
            {
                case Providers.SqlServer:
                    
                    this.DeleteCommand = objCommandBuilder.GetDeleteCommand();
                    break;
                case Providers.OleDb:
                    break;
                case Providers.OracleDataAccess:
                    this.DeleteCommand = objCommandBuilder.GetDeleteCommand();
                    break;
                case Providers.ODBC:
                    this.DeleteCommand = objCommandBuilder.GetDeleteCommand();
                    break;
                case Providers.ConfigDefined:
                    break;
                case Providers.ODBC_DB2:
                    this.DeleteCommand = objCommandBuilder.GetDeleteCommand();
                    break;
                case Providers.OleDb_DB2:
                    this.DeleteCommand = objCommandBuilder.GetDeleteCommand();
                    break;
                case Providers.DB2_iSeries:
                    //this.DeleteCommand = objCommandBuilder.GetDeleteCommand();
                    break;
                case Providers.MySQL:
                    //this.UpdateCommand = objCommandBuilder.GetUpdateCommand();
                    break;
                default:
                    break;
            }

        }

        //public void Init(Providers provider)
        //{
        //    ExecutionResult.Context = "DbObject:Init(Provide)";
        //    Init(ConfigurationManager.ConnectionStrings["connectionstring"].ConnectionString, provider);
        //    CaseSensitiveQuery(false);
        //}

        //public void Init(string connectionstring)
        //{
        //    ExecutionResult.Context = "DbObject:Init(Connectionstring)";
        //    Init(connectionstring, Providers.SqlServer);
        //    CaseSensitiveQuery(false);
        //}

        public void Init(DbConfig DbConfig)
        {
            ExecutionResult.Context = "DbObject:Init(DbConfig)";
            mDbConfig = DbConfig;

            if (mDbConfig.DbConnection is null)
            {
                mDbConfig.Init();
            }

            DbConnection = mDbConfig.DbConnection;
            objFactory = mDbConfig.DbProviderFactory;

            Init(mDbConfig.ConnectionString, mDbConfig.Provider);

            if (mDbConfig.DbObjects.ContainsKey(this.Name) == false)
            {
                DbConfig.DbObjects.Add(this.Name, this);
            }
            
            CaseSensitiveQuery(false);
        }

        //public void Init()
        //{
        //    ExecutionResult.Context = "DbObject:Init";
        //    Init(ConfigurationManager.ConnectionStrings["connectionstring"].ConnectionString, Providers.ConfigDefined);
        //    CaseSensitiveQuery(false);
        //}

        public void EnsureValidationRules()
        {
            switch (RuntimeUI)
            {
                case RuntimeUI.Wisej:
                    {
                        if (ValidatorLabelControl is null)
                        {
                            ValidatorLabelControl = Activator.CreateInstance(Type.GetType("Wisej.Web.Label"));
                        }

                        break;
                    }

                case RuntimeUI.WindowsForms:
                    {
                        if (ValidatorLabelControl is null)
                        {
                            ValidatorLabelControl = Activator.CreateInstance(Type.GetType("System.Windows.Forms.Label"));
                        }

                        break;
                    }

                default:
                    {
                        break;
                    }
            }

            mDbColumnsWithValidator = new List<DbColumn>();
            switch (RuntimeUI)
            {
                case RuntimeUI.Service:
                    {
                        break;
                    }

                case RuntimeUI.Wisej:
                case RuntimeUI.WindowsForms:
                    {
                        if (ValidatorLabelControl is object)
                        {
                            Type t1 = ValidatorLabelControl.GetType();
                            dynamic ValidatorLabel;
                            System.Drawing.Size textSize;
                            foreach (DbColumn DbColumn in mDbColumns)
                            {
                                if (DbColumn.ValidationType != ValidationTypes.None)
                                {
                                    foreach (BoundControl BoundControl in DbColumn.BoundControls)
                                    {
                                        ValidatorLabel = new object();
                                        ValidatorLabel = Activator.CreateInstance(t1);
                                        ValidatorLabel.Parent = BoundControl.Control.Parent;
                                        textSize = new System.Drawing.Size();// System.Windows.Forms.TextRenderer.MeasureText("!", (System.Drawing.Font)ValidatorLabel.Font);
                                        textSize.Width = 10;
                                        ValidatorLabel.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
                                        ValidatorLabel.Width = textSize.Width;
                                        ValidatorLabel.Height = BoundControl.Control.Height;
                                        ValidatorLabel.Top = BoundControl.Control.Top;
                                        ValidatorLabel.Left = BoundControl.Control.Left + BoundControl.Control.Width - ValidatorLabel.Width;
                                        ValidatorLabel.Font = BoundControl.Control.Font;
                                        ValidatorLabel.BackColor = System.Drawing.Color.Red;
                                        ValidatorLabel.ForeColor = System.Drawing.Color.White;
                                        ValidatorLabel.Text = "!";
                                        ValidatorLabel.Visible = false;
                                        dynamic evt_Click = ValidatorLabel.GetType().GetEvent("Click");
                                        evt_Click.RemoveEventHandler(ValidatorLabel, new EventHandler(ValidatorLabel_Click));
                                        evt_Click.AddEventHandler(ValidatorLabel, new EventHandler(ValidatorLabel_Click));
                                        dynamic evt_Leave = BoundControl.Control.GetType().GetEvent("Leave");
                                        evt_Leave -= BoundControl.Control.eAddNewEventHandler(DbColumn.ValidateBoundControls());
                                        evt_Leave += BoundControl.Control.eAddNewEventHandler(DbColumn.ValidateBoundControls());
                                        BoundControl.ValidationLabelControl = ValidatorLabel;
                                    }

                                    mDbColumnsWithValidator.Add(DbColumn);
                                }
                            }
                        }
                        else
                        {
                            // nothing to do
                        }

                        break;
                    }

                case RuntimeUI.Web:
                    {
                        break;
                    }
            }
        }

        private void ValidatorLabel_Click(dynamic sender, EventArgs e)
        {
            if (RedirectErrorsNotificationTo is object)
            {
                //Utilities.CallByName(RedirectErrorsNotificationTo, "Show", Utilities.CallType.Method, sender.Tag);
               Interaction.CallByName(RedirectErrorsNotificationTo, "Show", CallType.Method, sender.Tag);
            }
            //else
            //{
            //    //Interaction.MsgBox(sender.Tag, MsgBoxStyle.Exclamation, "Data Validation");
            //    .Show(sender.Tag, MsgBoxStyle.Exclamation, "Data Validation");
            //}
        }

        public int ValidateBoundControls()
        {
            // torna 0 se tutti i controlli sono validi
            int i = 0;
            if (mDataBinding == DataBoundControlsBehaviour.BasicDALDataBinding)
            {
                foreach (DbColumn DbColumn in mDbColumnsWithValidator)
                {
                    if (DbColumn.ValidationType != ValidationTypes.None)
                    {
                        if (DbColumn.ValidateBoundControls() == false)
                        {
                            i = i + 1;
                        }
                    }
                }
            }

            return Convert.ToInt32(i);
        }
        #endregion

        #region Error Management and Logging Methods
        public bool HandleErrors
        {
            get
            {
                ExecutionResult.Context = "DbObject:HandleErrors Get";
                return mHandleErrors;
            }

            set
            {
                ExecutionResult.Context = "DbObject:HandleErrors Set";
                mHandleErrors = value;
            }
        }

        public void ResetError()
        {
            ExecutionResult.Context = "DbObject:ResetError Get";
            mErrorState = false;
            mLastError = "";
            mLasteException = null;
            mLastErrorCode = 0;
            ExecutionResult.Reset();
        }

        public string LastError
        {
            get
            {
                return mLastError;
            }
        }

        public string LastErrorComplete
        {
            get
            {
                return mLastErrorComplete;
            }
        }

        public int LastErrorCode
        {
            get
            {
                return mLastErrorCode;
            }
        }

        public Exception LastErrorException
        {
            get
            {
                return mLasteException;
            }
        }


        private bool mAuditingEnabled = false;
        public bool AuditingEnabled
        {
            get
            {
                return mAuditingEnabled;
            }
            set
            {
                mAuditingEnabled = value;
            }
        }

        private void DuplicateKeyMessage(DbColumns PKeysDbColumns)
        {
            ExecutionResult.Context = "DbObject:DuplicateKeyMessage";
            string errmsg;
            string vbCrLf = "\r\n";


            errmsg = "Warning!" + vbCrLf;
            errmsg = errmsg + "Table <" + DbTableName + ">" + vbCrLf;
            errmsg = errmsg + "already exist a row with same primary key values." + vbCrLf;
            errmsg = errmsg + "Please check data value on the following columns:" + vbCrLf + vbCrLf;
            foreach (DbColumn dbcolumn in PKeysDbColumns)
                errmsg = errmsg + dbcolumn.FriendlyName + vbCrLf;
            if (mSuppressErrorsNotification == false)
            {
                if (mRedirectErrorsNotificationTo != null)
                {
                    //Interaction.CallByName(mRedirectErrorsNotificationTo, "Show", CallType.Method, errmsg);
                    Interaction.CallByName(mRedirectErrorsNotificationTo, "Show", CallType.Method, errmsg);
                }
                //else
                //{
                //    // MsgBox(errmsg, MsgBoxStyle.Exclamation, "Avviso per dati esistenti")
                //    Interaction.MsgBox(errmsg, MsgBoxStyle.Exclamation, "Warning for duplicate data.");
                //}
            }
        }

        private void HandleExceptions(Exception ex, string Context = "")
        {
            string errmsg = "";
            string vbCrLf = "\r\n";


            ExecutionResult.Reset();
            mLasteException = ex;

            errmsg = errmsg + "Database Name   : " + mDbConfig.DataBaseName + vbCrLf;
            errmsg = errmsg + "Database Object : " + GetType().Name + vbCrLf;
            switch (DbObjectType)
            {
                case DbObjectTypeEnum.Table:
                    {
                        errmsg = errmsg + "Table        : " + mDbTableName + vbCrLf;
                        break;
                    }

                case DbObjectTypeEnum.View:
                    {
                        errmsg = errmsg + "View         : " + mDbTableName + vbCrLf;
                        break;
                    }

                case DbObjectTypeEnum.InMemoryJoin:
                    {
                        errmsg = errmsg + "InMemoryJoin : " + mDbTableName + vbCrLf;
                        break;
                    }

                case DbObjectTypeEnum.SQLQuery:
                    {
                        errmsg = errmsg + "SQLQuery     : " + vbCrLf;
                        break;
                    }

                case DbObjectTypeEnum.StoredProcedure:
                    {
                        errmsg = errmsg + "Stored Proc. : " + mDbStoredProcedureName + vbCrLf;
                        break;
                    }

                case DbObjectTypeEnum.TableFunction or DbObjectTypeEnum.ScalarFunction:
                    {
                        errmsg = errmsg + "Function     : " + mDbFunctionName + vbCrLf;
                        break;
                    }
            }

            errmsg = errmsg + "Server Name  : " + mDbConfig.ServerName + vbCrLf;
            errmsg = errmsg + "DB Provider  : " + mDbConfig.Provider.ToString() + vbCrLf;
            errmsg = errmsg + "Username     : " + mDbConfig.UserName + vbCrLf;
            errmsg = errmsg + vbCrLf;
            errmsg = errmsg + "Context      : " + Context + vbCrLf;
            errmsg = errmsg + "Error Source : " + ex.Source + vbCrLf;
            errmsg = errmsg + "Error Code   : " + Information.Err().Number + vbCrLf;
            errmsg = errmsg + "Error Descr. : " + Information.Err().Description + vbCrLf + vbCrLf;
            errmsg = errmsg + "Exception    : " + ex.Message + vbCrLf + vbCrLf;



            mLastError = Information.Err().Description;
           
            mLastErrorComplete = errmsg;
            mLastErrorCode = Information.Err().Number;
            ErrorState = true;
            ExecutionResult.LastDllError = Information.Err().LastDllError;
            ExecutionResult.ResultCode = ExecutionResult.eResultCode.Failed;
            ExecutionResult.ResultMessage = ex.Message;
            ExecutionResult.ErrorCode = Information.Err().Number;
            ExecutionResult.Exception = ex;
            if (mLogError)
            {
                WriteToLog(ex.Message);
            }

            if (HandleErrors)
            {
                if (mSuppressErrorsNotification == false)
                {
                    if (mRedirectErrorsNotificationTo != null)
                    {
                        Interaction.CallByName(mRedirectErrorsNotificationTo, "Show", CallType.Method, errmsg);
                    }
                    //else
                    //{
                    //    Interaction.MsgBox(errmsg, MsgBoxStyle.Exclamation, "Error Reading o Writing Data!");
                    //}
                }
            }
            else
            {
                throw ex;
            }
        }

        public void SaveDataReaderAsCSV(string filename, string sepChar, bool FirstRowWithColumnName = true, DbDataReader DataReader = null, DbColumn[] DBColumns = null)
        {
            ExecutionResult.Context = "DbObject:SaveDataReaderAsCSV";
            if (DataReader is null)
            {
                // Me.LoadAll()
                DataReader = objDataReader;
            }

            StreamWriter writer = null;
            int i = 0;
            bool Cancel = false;
            int csvrownumber = 0;
            try
            {
                if (File.Exists(filename) == true)
                {
                    File.Delete(filename);
                }

                writer = new StreamWriter(filename);
                string sep = "";
                var builder = new StringBuilder();
                if (FirstRowWithColumnName == true)
                {
                    // first write a line with the columns name
                    if (DBColumns is null)
                    {
                        var loopTo = DataReader.FieldCount - 1;
                        for (i = 0; i <= loopTo; i++)
                        {
                            builder.Append(sep).Append(DataReader.GetName(i));
                            sep = sepChar;
                        }
                    }
                    else
                    {
                        foreach (DbColumn col in DBColumns)
                        {
                            builder.Append(sep).Append(col.DbColumnName);
                            sep = sepChar;
                        }
                    }

                    writer.WriteLine(builder.ToString());
                }

                if (DataReader.HasRows)
                {
                    var values = new object[DataReader.FieldCount];
                    int f = DataReader.FieldCount - 1;
                    // then write all the rows

                    while (DataReader.Read())
                    {
                        sep = "";
                        Cancel = false;
                        Array.Clear(values, 0, f);
                        DataReader.GetValues(values);
                        builder = new StringBuilder();
                        if (DBColumns is null)
                        {
                            var loopTo1 = DataReader.FieldCount - 1;
                            for (i = 0; i <= loopTo1; i++)
                            {
                                builder.Append(sep).Append(values[i]);
                                sep = sepChar;
                            }
                        }
                        else
                        {
                            foreach (DbColumn col in DBColumns)
                            {
                                builder.Append(sep).Append(DataReader[col.DbColumnName]);
                                sep = sepChar;
                            }
                        }

                        writer.WriteLine(builder.ToString());
                        csvrownumber = csvrownumber + 1;
                        if (DisableEvents == false)
                        {
                            WriteCSVRow?.Invoke(csvrownumber, ref Cancel);
                        }

                        if (Cancel == true)
                            break;
                    }
                }
            }
            finally
            {
                if (writer is object)
                    writer.Close();
            }
        }

        public void SaveAsXML(string XMLFile, bool IncludeSchema = false)
        {
            ExecutionResult.Context = "DbObject:SaveAsXML";
            if (string.IsNullOrEmpty(XMLFile))
                return;
            DoQuery();
            foreach (DbColumn dbc in mDbColumns)
                objDataTable.Columns[dbc.DbColumnNameE].DefaultValue = dbc.DefaultValue;
            try
            {
                switch (IncludeSchema)
                {
                    case true:
                        {
                            objDataTable.WriteXml(XMLFile, XmlWriteMode.WriteSchema);
                            break;
                        }

                    case false:
                        {
                            objDataTable.WriteXml(XMLFile);
                            break;
                        }
                }
            }
            catch (Exception ex)
            {
                ex = ex;
                ExecutionResult.Exception = ex;
                mErrorState = true;
                mLastError = ex.Message;
                mLasteException = ex;
                mLastErrorCode = 1999;
            }
        }

        public void SaveDataTableAsXML(string XMLFile, bool IncludeSchema = false)
        {
            ExecutionResult.Context = "DbObject:SaveDataTableAsXML";
            if (string.IsNullOrEmpty(XMLFile))
                return;
            DoQuery();
            foreach (DbColumn dbc in mDbColumns)
                objDataTable.Columns[dbc.DbColumnNameE].DefaultValue = dbc.DefaultValue;
            try
            {
                switch (IncludeSchema)
                {
                    case true:
                        {
                            objDataTable.WriteXml(XMLFile, XmlWriteMode.WriteSchema);
                            break;
                        }

                    case false:
                        {
                            objDataTable.WriteXml(XMLFile);
                            break;
                        }
                }
            }
            catch (Exception ex)
            {
                HandleExceptions(ex, ExecutionResult.Context);
                mErrorState = true;
                mLastError = ex.Message;
                mLasteException = ex;
                mLastErrorCode = 1999;
            }
        }

        public string GetDataTableAsXML(bool IncludeSchema = false)
        {
            ExecutionResult.Context = "DbObject:GetDataTableAsXML";
            DoQuery();
            foreach (DbColumn dbc in mDbColumns)
                objDataTable.Columns[dbc.DbColumnNameE].DefaultValue = dbc.DefaultValue;
            var str = new MemoryStream();
            switch (IncludeSchema)
            {
                case true:
                    {
                        objDataTable.WriteXml(str, XmlWriteMode.WriteSchema);
                        break;
                    }

                case false:
                    {
                        objDataTable.WriteXml(str);
                        break;
                    }
            }

            str.Seek(0L, SeekOrigin.Begin);
            var sr = new StreamReader(str);
            string xmlstr;
            xmlstr = sr.ReadToEnd();
            var xml = new System.Xml.XmlDocument();
            xml.LoadXml(xmlstr);
            // xmlstr = xml.ChildNodes(0).InnerXml.ToString
            // xmlstr = xmlstr.Replace(xml.ChildNodes(0).Name, "DATATABLE")

            return xml.InnerXml;
        }

        public void SaveDataTableAsCSV(string filename, string sepChar = ";", bool FirstRowWithColumnName = true, DataTable DataTable = null, DbColumns DBColumns = null)
        {
            ExecutionResult.Context = "DbObject:SaveDataTableAsCSV";
            SaveAsCSV(filename, sepChar, FirstRowWithColumnName, DataTable, DBColumns);
        }

        public MemoryStream SaveDataTableAsCSVMemoryStream(string sepChar = ";", bool FirstRowWithColumnName = true, DataTable DataTable = null, DbColumns DBColumns = null)
        {
            ExecutionResult.Context = "DbObject:SaveDataTableAsCSVMemoryStream";
            return SaveAsCSVMemoryStream(sepChar, FirstRowWithColumnName, DataTable, DBColumns);
        }

        public void SaveAsCSV(string filename, string sepChar = ";", bool FirstRowWithColumnName = true, DataTable DataTable = null, DbColumns DBColumns = null)
        {
            ExecutionResult.Context = "DbObject:SaveAsCSV";
            if (DataTable is null)
                DataTable = GetDataTable();
            StreamWriter writer = null;
            bool Cancel = false;
            int CsvRowNumber = 0;
            string sep = "";
            try
            {
                writer = new StreamWriter(filename);
                // first write a line with the columns name

                var builder = new StringBuilder();
                if (DBColumns is null)
                {
                    foreach (DbColumn col in GetDbColumns())
                    {
                        builder.Append(sep).Append("\"").Append(col.FriendlyName).Append("\"");
                        sep = sepChar;
                    }
                }
                else
                {
                    foreach (DbColumn col in DBColumns)
                    {
                        builder.Append(sep).Append("\"").Append(col.FriendlyName).Append("\"");
                        sep = sepChar;
                    }
                }

                writer.WriteLine(builder.ToString());
                string v = "";
                string quote = "\"";
                string doublequote = quote + quote;
                TypeCode TypeCode;
                // then write all the rows
                foreach (DataRow row in DataTable.Rows)
                {
                    sep = "";
                    Cancel = false;
                    builder = new StringBuilder();
                    if (DBColumns is null)
                    {
                        foreach (DataColumn col in DataTable.Columns)
                        {
                            v = row[col.ColumnName].ToString(); // .Replace(a, aa)
                            TypeCode = Type.GetTypeCode(col.DataType);
                            switch (TypeCode)
                            {
                                case System.TypeCode.Byte:
                                case System.TypeCode.SByte:
                                case System.TypeCode.Int16:
                                case System.TypeCode.UInt16:
                                case System.TypeCode.Int32:
                                case System.TypeCode.UInt32:
                                case System.TypeCode.Int64:
                                case System.TypeCode.UInt64:
                                case System.TypeCode.Single:
                                case System.TypeCode.Double:
                                case System.TypeCode.Decimal:
                                    {
                                        builder.Append(sep).Append(v);
                                        break;
                                    }

                                case System.TypeCode.String:
                                case System.TypeCode.Char:
                                    {
                                        if (v.Contains(quote))
                                        {
                                            v = v.Replace(quote, doublequote);
                                        }

                                        builder.Append(sep).Append(quote).Append(v).Append(quote);
                                        break;
                                    }

                                case System.TypeCode.Boolean:
                                    {
                                        builder.Append(sep).Append(v);
                                        break;
                                    }

                                case System.TypeCode.DateTime:
                                    {
                                        builder.Append(sep).Append(v);
                                        break;
                                    }

                                default:
                                    {
                                        builder.Append(sep).Append(v);
                                        break;
                                    }
                            }

                            sep = sepChar;
                        }
                    }
                    else
                    {
                        foreach (DbColumn col in DBColumns)
                        {
                            v = row[col.DbColumnNameE].ToString();
                            TypeCode = Type.GetTypeCode(col.DataType);
                            switch (TypeCode)
                            {
                                case System.TypeCode.Byte:
                                case System.TypeCode.SByte:
                                case System.TypeCode.Int16:
                                case System.TypeCode.UInt16:
                                case System.TypeCode.Int32:
                                case System.TypeCode.UInt32:
                                case System.TypeCode.Int64:
                                case System.TypeCode.UInt64:
                                case System.TypeCode.Single:
                                case System.TypeCode.Double:
                                case System.TypeCode.Decimal:
                                    {
                                        builder.Append(sep).Append(v);
                                        break;
                                    }

                                case System.TypeCode.String:
                                case System.TypeCode.Char:
                                    {
                                        // If (v.Contains("""")) Then
                                        // v = QuoteValue(v)
                                        // End If
                                        builder.Append(sep).Append("\"").Append(v).Append("\"");
                                        break;
                                    }

                                case System.TypeCode.Boolean:
                                    {
                                        builder.Append(sep).Append(v);
                                        break;
                                    }

                                case System.TypeCode.DateTime:
                                    {
                                        builder.Append(sep).Append(v);
                                        break;
                                    }

                                default:
                                    {
                                        builder.Append(sep).Append(v);
                                        break;
                                    }
                            }

                            sep = sepChar;
                        }
                    }

                    writer.WriteLine(builder.ToString());
                    CsvRowNumber = CsvRowNumber + 1;
                    if (DisableEvents == false)
                    {
                        WriteCSVRow?.Invoke(CsvRowNumber, ref Cancel);
                    }

                    if (Cancel == true)
                        break;
                }
            }
            catch (Exception ex)
            {
                ExecutionResult.Exception = ex;
                mErrorState = true;
                mLastError = ex.Message;
                mLasteException = ex;
                mLastErrorCode = 1;
            }

            if (writer is object)
                writer.Close();
        }

        public MemoryStream SaveAsCSVMemoryStream(string sepChar = ";", bool FirstRowWithColumnName = true, DataTable DataTable = null, DbColumns DBColumns = null)
        {
            ExecutionResult.Context = "DbObject: SaveAsCSVMemoryStream";
            if (DataTable is null)
                DataTable = GetDataTable();
            StreamWriter writer = null;
            bool Cancel = false;
            int CsvRowNumber = 0;
            string sep = "";
            var stream = new MemoryStream();
            try
            {
                writer = new StreamWriter(stream);

                // first write a line with the columns name

                var builder = new StringBuilder();
                if (DBColumns is null)
                {
                    foreach (DbColumn col in GetDbColumns())
                    {
                        builder.Append(sep).Append("\"").Append(col.FriendlyName).Append("\"");
                        sep = sepChar;
                    }
                }
                else
                {
                    foreach (DbColumn col in DBColumns)
                    {
                        builder.Append(sep).Append("\"").Append(col.FriendlyName).Append("\"");
                        sep = sepChar;
                    }
                }

                writer.WriteLine(builder.ToString());
                string v = "";
                string quote = "\"";
                string doublequote = quote + quote;
                TypeCode TypeCode;

                // then write all the rows
                foreach (DataRow row in DataTable.Rows)
                {
                    sep = "";
                    Cancel = false;
                    builder = new StringBuilder();
                    if (DBColumns is null)
                    {
                        foreach (DataColumn col in DataTable.Columns)
                        {
                            v = row[col.ColumnName].ToString(); // .Replace(a, aa)
                            TypeCode = Type.GetTypeCode(col.DataType);
                            switch (TypeCode)
                            {
                                case System.TypeCode.Byte:
                                case System.TypeCode.SByte:
                                case System.TypeCode.Int16:
                                case System.TypeCode.UInt16:
                                case System.TypeCode.Int32:
                                case System.TypeCode.UInt32:
                                case System.TypeCode.Int64:
                                case System.TypeCode.UInt64:
                                case System.TypeCode.Single:
                                case System.TypeCode.Double:
                                case System.TypeCode.Decimal:
                                    {
                                        builder.Append(sep).Append(v);
                                        break;
                                    }

                                case System.TypeCode.String:
                                case System.TypeCode.Char:
                                    {
                                        if (v.Contains(quote))
                                        {
                                            v = v.Replace(quote, doublequote);
                                        }

                                        builder.Append(sep).Append(quote).Append(v).Append(quote);
                                        break;
                                    }

                                case System.TypeCode.Boolean:
                                    {
                                        builder.Append(sep).Append(v);
                                        break;
                                    }

                                case System.TypeCode.DateTime:
                                    {
                                        builder.Append(sep).Append(v);
                                        break;
                                    }

                                default:
                                    {
                                        builder.Append(sep).Append(v);
                                        break;
                                    }
                            }

                            sep = sepChar;
                        }
                    }
                    else
                    {
                        foreach (DbColumn col in DBColumns)
                        {
                            v = row[col.DbColumnNameE].ToString();
                            TypeCode = Type.GetTypeCode(col.DataType);
                            switch (TypeCode)
                            {
                                case System.TypeCode.Byte:
                                case System.TypeCode.SByte:
                                case System.TypeCode.Int16:
                                case System.TypeCode.UInt16:
                                case System.TypeCode.Int32:
                                case System.TypeCode.UInt32:
                                case System.TypeCode.Int64:
                                case System.TypeCode.UInt64:
                                case System.TypeCode.Single:
                                case System.TypeCode.Double:
                                case System.TypeCode.Decimal:
                                    {
                                        builder.Append(sep).Append(v);
                                        break;
                                    }

                                case System.TypeCode.String:
                                case System.TypeCode.Char:
                                    {
                                        // If (v.Contains("""")) Then
                                        // v = QuoteValue(v)
                                        // End If
                                        builder.Append(sep).Append("\"").Append(v).Append("\"");
                                        break;
                                    }

                                case System.TypeCode.Boolean:
                                    {
                                        builder.Append(sep).Append(v);
                                        break;
                                    }

                                case System.TypeCode.DateTime:
                                    {
                                        builder.Append(sep).Append(v);
                                        break;
                                    }

                                default:
                                    {
                                        builder.Append(sep).Append(v);
                                        break;
                                    }
                            }

                            sep = sepChar;
                        }
                    }

                    writer.WriteLine(builder.ToString());
                    CsvRowNumber = CsvRowNumber + 1;
                    if (DisableEvents == false)
                    {
                        WriteCSVRow?.Invoke(CsvRowNumber, ref Cancel);
                    }

                    if (Cancel == true)
                        break;
                }
            }
            catch (Exception ex)
            {
                ExecutionResult.Exception = ex;
                mErrorState = true;
                mLastError = ex.Message;
                mLasteException = ex;
                mLastErrorCode = 1;
            }
            finally
            {
                // If Not writer Is Nothing Then writer.Close()
            }

            return stream;
        }


        public string SaveCurrentRowAsJson()
        {
            return Utilities.DataTableHelper.DataRowToJson(this.CurrentDataRow);
        }

        public string SaveAsJson()
        {
            
            return Utilities.DataTableHelper.DataTableToJson (this.DataTable);
        }
        
        private void WriteToLog(string msg)
        {
            ExecutionResult.Context = "DbObject:WriteLog";
            try
            {
                var writer = File.AppendText(LogFile);
                writer.WriteLine(DateTime.Now.ToString() + " - " + msg);
                writer.Close();
            }
            catch
            {
            }
        }
        #endregion

        #region AddParameter Method
        private int AddParameter(string name, object value)
        {
            ExecutionResult.Context = "DbObject:AddParameter";
            var p = objFactory.CreateParameter();
            p.ParameterName = name;
            p.Value = value;
            return objCommand.Parameters.Add(p);
        }

        private int AddParameter(Parameter parameter)
        {
            ExecutionResult.Context = "DbObject:AddParameter";
            return objCommand.Parameters.Add(parameter);
        }
        #endregion

        public DbCommand Command
        {
            get
            {
                ExecutionResult.Context = "DbObject:Command Get";
                return objCommand;
            }

            set
            {
                ExecutionResult.Context = "DbObject:Command Set";
                objCommand = value;
            }
        }

        public DbParameterCollection StoreProcedureParameters
        {
            get
            {
                ExecutionResult.Context = "DbObject:StoredProcedureParameters Get";
                return objStoredProcedureCommand.Parameters;
            }

            set
            {
                ExecutionResult.Context = "DbObject:StoredProcedureParameters Set";
                objStoredProcedureCommand.Parameters.Clear();
                foreach (DbParameter param in value)
                    objStoredProcedureCommand.Parameters.Add(param.IDbParameter);
            }
        }

        public DbCommand ExecuteCommand
        {
            get
            {
                ExecutionResult.Context = "DbObject:ExecuteCommand Get";
                return objExecuteCommand;
            }

            set
            {
                ExecutionResult.Context = "DbObject:ExecuteCommand Set";
                objExecuteCommand = value;
            }
        }

        private DataSet ExecuteStoredProcedure()
        {
            ExecutionResult.Context = "DbObject:ExecuteStoredProcedure";
            var ds = new DataSet();
            {
                ref var withBlock = ref objStoredProcedureCommand;
                withBlock.CommandText = mDbStoredProcedureName;
                withBlock.CommandType = CommandType.StoredProcedure;
                withBlock.Connection = DbConnection;
            }

            objAdapter.SelectCommand = objStoredProcedureCommand;
            objAdapter.SelectCommand.CommandType = CommandType.StoredProcedure;
            int i = -1;
            try
            {
                if (DbConnection.State == System.Data.ConnectionState.Closed)
                {
                    DbConnection.Open();
                }

                switch (mDbConfig.Provider)
                {
                    case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                        {
                            if (objStoredProcedureCommand.Transaction is null)
                            {
                                objStoredProcedureCommand.Transaction = DbConfig.DbTransaction;
                            }

                            break;
                        }

                    default:
                        {
                            if (objStoredProcedureCommand.Transaction is null)
                            {
                                objStoredProcedureCommand.Transaction = DbConfig.DbTransaction;
                            }

                            break;
                        }
                }

                try
                {
                    // PUNTOCRITICO
                    mRowCount = objAdapter.Fill(ds, "Table");
                    if (ds.Tables.Count > 0)
                    {
                        ds.Tables[0].TableName = DbTableName;
                    }
                }
                catch (Exception ex)
                {
                    HandleExceptions(ex);
                }
                finally
                {
                }
            }

            // i = Me.objStoredProcedureCommand.ExecuteNonQuery

            catch (Exception ex)
            {
                HandleExceptions(ex);
            }
            finally
            {
            }

            return ds;
        }

        private DataSet ExecuteTableScalarFunction()
        {
            ExecutionResult.Context = "DbObject:ExecuteTableScalarFunction";
            var ds = new DataSet();
            {
                ref var withBlock = ref objStoredProcedureCommand;
                switch (mDbObjectType)
                {
                    case DbObjectTypeEnum.TableFunction:
                        {
                            withBlock.CommandText = "SELECT * FROM " + mDbFunctionName;
                            break;
                        }

                    case DbObjectTypeEnum.ScalarFunction:
                        {
                            withBlock.CommandText = "SELECT " + mDbFunctionName;
                            break;
                        }

                    default:
                        {
                            return null;
                        }
                }

                withBlock.CommandType = CommandType.Text;
                withBlock.Connection = DbConnection;
            }

            objAdapter.SelectCommand = objStoredProcedureCommand;
            int i = -1;
            try
            {
                if (DbConnection.State == System.Data.ConnectionState.Closed)
                {
                    DbConnection.Open();
                }

                switch (mDbConfig.Provider)
                {
                    case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                        {
                            if (objStoredProcedureCommand.Transaction is null)
                            {
                                objStoredProcedureCommand.Transaction = DbConfig.DbTransaction;
                            }

                            break;
                        }

                    default:
                        {
                            if (objStoredProcedureCommand.Transaction is null)
                            {
                                objStoredProcedureCommand.Transaction = DbConfig.DbTransaction;
                            }

                            break;
                        }
                }

                try
                {
                    // PUNTOCRITICO
                    mRowCount = objAdapter.Fill(ds, DbTableName);
                    if (ds.Tables.Count > 0)
                    {
                        ds.Tables[0].TableName = DbTableName;
                        ds.Tables[0].Columns[0].ColumnName = "RETURN_VALUE";
                    }
                }
                catch (Exception ex)
                {
                    HandleExceptions(ex);
                }
                finally
                {
                }
            }
            catch (Exception ex)
            {
                HandleExceptions(ex);
            }
            finally
            {
            }

            return ds;
        }

        public DbCommand InsertCommand
        {
            get
            {
                return objInsertCommand;
            }

            set
            {
                objInsertCommand = value;
            }
        }

        public DbCommand UpdateCommand
        {
            get
            {
                return objUpdateCommand;
            }

            set
            {
                objUpdateCommand = value;
            }
        }

        public DbCommand DeleteCommand
        {
            get
            {
                return objDeleteCommand;
            }

            set
            {
                objDeleteCommand = value;
            }
        }
        #region Transaction Management Methods




        #endregion

        #region ExecuteNonQuery Method



        public int ExecuteNonQuery(string query)
        {
            ExecutionResult.Context = "DbObject:ExecuteNonQuery";
            return ExecuteNonQuery(query, CommandType.Text, ConnectionState.KeepOpen);
        }

        public int ExecuteNonQuery(string query, CommandType commandtype)
        {
            ExecutionResult.Context = "DbObject:ExecuteNonQuery";
            return ExecuteNonQuery(query, commandtype, ConnectionState.KeepOpen);
        }

        public int ExecuteNonQuery(string query, ConnectionState connectionstate)
        {
            ExecutionResult.Context = "DbObject:ExecuteNonQuery";
            return ExecuteNonQuery(query, CommandType.Text, connectionstate);
        }

        public int ExecuteNonQuery(string query, CommandType commandtype, ConnectionState connectionstate)
        {
            ExecutionResult.Context = "DbObject:ExecuteNonQuery";
            DbCommand lobjCommand;
            lobjCommand = objFactory.CreateCommand();
            lobjCommand.CommandText = query;
            lobjCommand.CommandType = commandtype;
            lobjCommand.Connection = DbConnection;
            int i = -1;
            try
            {
                if (DbConnection.State == System.Data.ConnectionState.Closed)
                {
                    DbConnection.Open();
                }

                i = lobjCommand.ExecuteNonQuery();
            }

            // If Me.DbProvider = Providers.OracleDataAccess Then
            // ManageOraclePools(Me.DBConnection)
            // End If


            catch (Exception ex)
            {
                HandleExceptions(ex);
            }
            finally
            {

                // lobjCommand.Parameters.Clear()

                lobjCommand.Dispose();
            }

            if (mDbConfig.DbConnectionKeepOpen == false)
                DbConnection.Close();
            
            return i;
        }
        #endregion

        #region ExecuteScalar Method
        #region ExecuteScalar Overload
        public object ExecuteScalar(string query)
        {
            ExecutionResult.Context = "DbObject:ExecuteScalar 1";
            return ExecuteScalar(query, CommandType.Text, ConnectionState.KeepOpen);
        }

        public object ExecuteScalar(string query, CommandType commandtype)
        {
            ExecutionResult.Context = "DbObject:ExecuteScalar 2";
            return ExecuteScalar(query, commandtype, ConnectionState.KeepOpen);
        }

        public object ExecuteScalar(string query, ConnectionState connectionstate)
        {
            ExecutionResult.Context = "DbObject:ExecuteScalar";
            return ExecuteScalar(query, CommandType.Text, connectionstate);
        }
        #endregion

        public object ExecuteScalar(string query, CommandType commandtype, ConnectionState connectionstate)
        {
            ExecutionResult.Context = "DbObject:ExecuteScalar 3";
            DbCommand lobjCommand;
            lobjCommand = objFactory.CreateCommand();
            lobjCommand = objFactory.CreateCommand();
            lobjCommand.CommandText = query;
            lobjCommand.CommandType = commandtype;
            lobjCommand.Connection = DbConnection;
            object i = null;
            try
            {
                if (DbConnection.State == System.Data.ConnectionState.Closed)
                {
                    DbConnection.Open();
                }

                i = lobjCommand.ExecuteScalar();
            }

            // If Me.DbProvider = Providers.OracleDataAccess Then
            // ManageOraclePools(Me.DBConnection)
            // End If


            catch (Exception ex)
            {
                HandleExceptions(ex);
            }
            finally
            {
                lobjCommand.Parameters.Clear();
                lobjCommand.Dispose();
            }
            if (mDbConfig.DbConnectionKeepOpen == false)
                DbConnection.Close();
            return i;
        }

        public object ExecuteScalar(string query, DbParameterCollection Parameters, CommandType commandtype, ConnectionState connectionstate)
        {
            ExecutionResult.Context = "DbObject:ExecuteScalar 4";
            DbCommand lobjCommand;
            lobjCommand = objFactory.CreateCommand();
            DbParameterCollection xParameters = null;
            lobjCommand.CommandText = query;
            lobjCommand.CommandType = commandtype;
            lobjCommand.Connection = DbConnection;
            foreach (DbParameter parameter in Parameters)
            {
                DbParameter xparameter = new DbParameter();
                xparameter.IDbParameter = lobjCommand.CreateParameter();
                xparameter.ParameterName = parameter.ParameterName;
                xparameter.DbType = parameter.DbType;
                xparameter.Value = parameter.Value;
            }

            object i = null;
            try
            {
                if (DbConnection.State == System.Data.ConnectionState.Closed)
                {
                    DbConnection.Open();
                }

                i = lobjCommand.ExecuteScalar();
            }
            // If Me.DbProvider = Providers.OracleDataAccess Then
            // ManageOraclePools(Me.DBConnection)
            // End If


            catch (Exception ex)
            {
                HandleExceptions(ex);
            }
            finally
            {
                lobjCommand.Parameters.Clear();
                lobjCommand.Dispose();
            }
            if (mDbConfig.DbConnectionKeepOpen == false)
                DbConnection.Close();
            return i;
        }


        #endregion

        #region ExecuteReader Method
        #region ExecuteReader Overloads
        public DbDataReader ExecuteReader()
        {
            ExecutionResult.Context = "DbObject:ExecuteReader 1";
            return ExecuteReader("", CommandType.Text, ConnectionState.KeepOpen);
        }

        public DbDataReader ExecuteReader(string query)
        {
            ExecutionResult.Context = "DbObject:ExecuteReader 2";
            return ExecuteReader(query, CommandType.Text, ConnectionState.KeepOpen);
        }

        public DbDataReader ExecuteReader(string query, ConnectionState connectionstate)
        {
            ExecutionResult.Context = "DbObject:ExecuteReader 3";
            return ExecuteReader(query, CommandType.Text, connectionstate);
        }
        #endregion

        public DbDataReader ExecuteReader(string query, CommandType commandtype, ConnectionState connectionstate = ConnectionState.KeepOpen)
        {
            ExecutionResult.Context = "DbObject:ExecuteReader";
            DbDataReader reader = null;

            // OGGETTI DEDICATI AL DATAREADER
            if (objDataReaderConnection==null)
                objDataReaderConnection = objFactory.CreateConnection();
            if (objDataReaderCommand==null)
                objDataReaderCommand = objFactory.CreateCommand();

            objDataReaderConnection.ConnectionString = mConnectionString;
            objDataReaderCommand.Connection = objDataReaderConnection;
            if (mDbObjectInitialized == false)
            {
                return reader;
            }

            if (Fields == null)
            {
                Fields = GetFields(this);
            }

            string sSELECT = "SELECT ";
            if (mDistinctClause == true)
            {
                sSELECT = "SELECT DISTINCT ";
            }

            string Query1 = sSELECT + FieldsList + " FROM " + DbTableName + " " + WithOptions + " ";
            if (mTopRecords > 0)
            {
                Query1 = sSELECT + FieldsList + " FROM " + DbTableName + " " + WithOptions + " ";
            }

            if (query.Trim() == "" || query == null)
            {
                objFiltersGroup.ParametersStyle = DbConfig.ParametersStyle;
                DbCommand argDBCommand = objDataReaderCommand;
                objFiltersGroup.BuildSQLFilter(ref query, ref argDBCommand, mDbConfig.Provider);
                objDataReaderCommand = (DbCommand)argDBCommand;
                SQLWhere = query;
                if (!string.IsNullOrEmpty(query))
                {
                    query = Query1 + " WHERE " + query;
                }
                else
                {
                    query = Query1;
                }

            }

            //if (!string.IsNullOrEmpty(mOrderBy))
            //    query = query + " ORDER BY " + mOrderBy;


            if (mDbOrderBy .Count==0)
            {
                if (!string.IsNullOrEmpty(mOrderBy))
                    query = query + " ORDER BY " + mOrderBy;

            }
            else
            {
                query = query + " ORDER BY " + mDbOrderBy.BuildOrderByClause();
            }
            
            


            if (mTopRecords > 0)
            {
                switch (mDbConfig.Provider)
                {
                    case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                        {
                            query = query + " FETCH FIRST " + mTopRecords + " ROWS ONLY ";
                            break;
                        }
                    case Providers.OracleDataAccess:

                        if (Convert.ToInt32(this.DbConfig.OracleVersion) >= 12)
                        {
                            query = query + " FETCH FIRST " + this.mTopRecords + " ROWS ONLY ";
                        }
                        else
                        {
                            query = "SELECT * FROM (" + query + ") WHERE ROWNUM <= " + this.mTopRecords;
                        }
                        break;



                    default:
                        {
                            break;
                        }
                }
            }

            SQLSelect = query;
            {
                ref var withBlock = ref objDataReaderCommand;
                withBlock.CommandText = query + AppendedSQL;
                withBlock.CommandType = commandtype;
                withBlock.Connection = objDataReaderConnection;
                withBlock.CommandTimeout = CommandTimeOut;
            }

            try
            {
                if (objDataReaderConnection.State == System.Data.ConnectionState.Closed)
                {
                    objDataReaderConnection.Open();
                }

                reader = objDataReaderCommand.ExecuteReader((CommandBehavior)connectionstate);
            }

            // If Me.DbProvider = Providers.OracleDataAccess Then
            // ManageOraclePools(Me.DBConnection)
            // End If



            catch (Exception ex)
            {
                HandleExceptions(ex);
            }
            finally
            {
            }

            return reader;
        }

        public bool GetDataTableFromDataReader(ref DataTable DataTable, string query)
        {
            ExecutionResult.Context = "DbObject:GetDataTableFromDataReader 1";
            return GetDataTableFromDataReader(ref DataTable, query, CommandType.Text, ConnectionState.KeepOpen);
        }

        public bool GetDataTableFromDataReader(ref DataTable DataTable, string query, CommandType commandtype, ConnectionState connectionstate)
        {
            ExecutionResult.Context = "DbObject:GetDataTableFromDataReader";
            bool status = false;
            DbDataReader reader = null;
            bool UseCachedConnection = false;
            // Dim lobjConnection As DbConnection
            DbCommand lobjCommand;
            // lobjConnection = Me.objFactory.CreateConnection
            lobjCommand = objFactory.CreateCommand();

            // If IsNothing(Me.mCachedConnection) = True Then
            // lobjConnection.ConnectionString = Me.mConnectionString

            // Else
            // UseCachedConnection = True
            // lobjConnection = mCachedConnection
            // End If


            // lobjCommand.Connection = lobjConnection


            if (mDbObjectInitialized == false)
            {
                return false;
            }

            if (Fields == null)
            {
                Fields = GetFields(this);
            }

            string sSELECT = "SELECT ";
            if (mDistinctClause == true)
            {
                sSELECT = "SELECT DISTINCT ";
            }

            string Query1 = sSELECT + FieldsList + " FROM " + DbTableName;
            if (mTopRecords > 0)
            {
                Query1 = sSELECT + " TOP (" + mTopRecords + ") " + FieldsList + " FROM " + DbTableName;
            }

            if (query.Trim() == "")
            {
                objFiltersGroup.ParametersStyle = mDbConfig.ParametersStyle;
                string argSQL = "";
                DbCommand argDBCommand = lobjCommand;
                objFiltersGroup.BuildSQLFilter(ref argSQL, ref argDBCommand, mDbConfig.Provider);
                lobjCommand = (DbCommand)argDBCommand;
                if (!string.IsNullOrEmpty(query))
                {
                    query = Query1 + " WHERE " + query;
                }
                else
                {
                    query = Query1;
                }

            }

            lobjCommand.CommandText = query;
            lobjCommand.CommandType = commandtype;
            lobjCommand.Connection = DbConnection; // lobjConnection
            try
            {
                // If lobjConnection.State = System.Data.ConnectionState.Closed Then
                // lobjConnection.Open()
                // End If

                if (DbConnection.State == System.Data.ConnectionState.Closed)
                {
                    DbConnection.Open();
                }

                if (connectionstate == ConnectionState.CloseOnExit)
                {
                    reader = lobjCommand.ExecuteReader(CommandBehavior.CloseConnection);
                }
                else
                {
                    reader = lobjCommand.ExecuteReader((CommandBehavior)connectionstate);
                }
            }
            catch (Exception ex)
            {
                HandleExceptions(ex);
            }
            finally
            {
                try
                {
                    DataTable.Load(reader);
                    status = true;
                }
                catch (Exception ex)
                {
                }

                lobjCommand.Parameters.Clear();
                switch (UseCachedConnection)
                {
                    case true:
                        {
                            break;
                        }

                    default:
                        {

                            // If Val(connectionstate) = connectionstate.CloseOnExit Then
                            // lobjConnection.Close()
                            // lobjConnection.Dispose()
                            // End If

                            if (connectionstate == ConnectionState.CloseOnExit)
                            {
                                DbConnection.Close();
                            }

                            break;
                        }
                }

                lobjCommand.Dispose();
            }

            return status;
        }
        #endregion

        #region ExecuteDataSet Method

        #region ExecuteDataSet Overloads
        public DataSet ExecuteDataSet()
        {
            ExecutionResult.Context = "DbObject:ExecuteDataSet_1";
            return ExecuteDataSet("", CommandType.Text, ConnectionState.KeepOpen);
        }

        public DataSet ExecuteDataSet(string query)
        {
            ExecutionResult.Context = "DbObject:ExecuteDataSet_2";
            return ExecuteDataSet(query, CommandType.Text, ConnectionState.KeepOpen);
        }

        public DataSet ExecuteDataSet(string query, CommandType commandtype)
        {
            ExecutionResult.Context = "DbObject:ExecuteDataSet_3";
            return ExecuteDataSet(query, commandtype, ConnectionState.KeepOpen);
        }

        public DataSet ExecuteDataSet(string query, ConnectionState connectionstate)
        {
            ExecutionResult.Context = "DbObject:ExecuteDataSet_4";
            return ExecuteDataSet(query, CommandType.Text, connectionstate);
        }
        #endregion

        public DataSet ExecuteDataSet(string query, CommandType commandtype, ConnectionState connectionstate)
        {
            ExecutionResult.Context = "DbObject:ExecuteDataSet";

            // Dim UseCachedConnection As Boolean = False
            var ds = new DataSet();
            string Query1 = "";
            string sSELECT = "SELECT ";
            if (mDistinctClause == true)
            {
                sSELECT = "SELECT DISTINCT ";
            }

            if (Fields == null)
            {
                Fields = GetFields(this);
            }

            switch (DbObjectType)
            {
                case DbObjectTypeEnum.Table or DbObjectTypeEnum.View:
                    {
                        switch (mTopRecords)
                        {
                            case 0:
                                {
                                    Query1 = sSELECT + FieldsList + " FROM " + DbTableName + " " + WithOptions + " ";
                                    break;
                                }

                            default:
                                {
                                    if (mDbConfig.SupportTopRecords == true)
                                    {
                                        switch (mDbConfig.Provider)
                                        {
                                            case Providers.SqlServer:
                                                {
                                                    Query1 = sSELECT + " TOP (" + mTopRecords + ") " + FieldsList + " FROM " + DbTableName + " " + WithOptions + " ";
                                                    break;
                                                }

                                            case Providers.OracleDataAccess:
                                                {
                                                    Query1 = sSELECT + this.FieldsList + " FROM " + this.DbTableName + " " + this.WithOptions + " ";
                                                    break;
                                                }
                                            default:
                                                {
                                                    Query1 = sSELECT + this.FieldsList + " FROM " + this.DbTableName + " " + this.WithOptions + " ";
                                                    break;
                                                }
                                        }
                                    }
                                    else
                                    {
                                        Query1 = sSELECT + FieldsList + " FROM " + DbTableName + " " + WithOptions + " ";
                                    }

                                    break;
                                }
                        }

                        break;
                    }

                case DbObjectTypeEnum.SQLQuery:
                    {
                        switch (mTopRecords)
                        {
                            case 0:
                                {

                                    switch (mDbConfig.Provider)
                                    {
                                        case Providers.OracleDataAccess:
                                            Query1 = sSELECT + FieldsList + " FROM (" + SQLQuery + ")";
                                            break;

                                        default:
                                            {
                                                Query1 = sSELECT + FieldsList + " FROM (" + SQLQuery + ")  as xTable";
                                                break;
                                            }
                                    }

                                    break;
                                }

                            default:
                                {
                                    if (mDbConfig.SupportTopRecords == true)
                                    {
                                        switch (mDbConfig.Provider)
                                        {
                                            case Providers.SqlServer:
                                                {

                                                    Query1 = sSELECT + " TOP (" + mTopRecords + ") " + FieldsList + " FROM (" + SQLQuery + ") as xTable";
                                                    break;
                                                }
                                            case Providers.OracleDataAccess:

                                                Query1 = sSELECT + FieldsList + " FROM (" + SQLQuery + ")";
                                                break;


                                            default:
                                                {
                                                    Query1 = sSELECT + FieldsList + " FROM (" + SQLQuery + ") as xTable";
                                                    break;
                                                }
                                        }
                                    }
                                    else
                                    {
                                        Query1 = sSELECT + FieldsList + " FROM (" + SQLQuery + ") as xTable";
                                    }

                                    break;
                                }
                        }

                        break;
                    }
            }
            // ???????????????????????????????????????????????????
            if (mDbObjectInitialized == false)
            {
                return ds;
            }


            objCommand.CommandTimeout = CommandTimeOut;

            objAdapter.SelectCommand = objCommand;
            objCommandBuilder.DataAdapter = objAdapter;
            if (mDbObjectType == DbObjectTypeEnum.Join)
            {
                Query1 = Convert.ToString(BuildSQLJoin());
            }

            if (query.Trim() == "")
            {
                objFiltersGroup.ParametersStyle = mDbConfig.ParametersStyle;
                DbCommand argDBCommand = objCommand;
                objFiltersGroup.BuildSQLFilter(ref query, ref argDBCommand, mDbConfig.Provider);
                objCommand = (DbCommand)argDBCommand;
                SQLWhere = query;
                if (!string.IsNullOrEmpty(query))
                {
                    query = Query1 + " WHERE " + query;
                }
                else
                {
                    query = Query1;
                }


            }

            //if (!string.IsNullOrEmpty(mOrderBy))
            //    query = query + " ORDER BY " + mOrderBy;
            if (mDbOrderBy.Count ==0)
            {
                if (!string.IsNullOrEmpty(mOrderBy))
                    query = query + " ORDER BY " + mOrderBy;

            }
            else
            {
                query = query + " ORDER BY " + mDbOrderBy.BuildOrderByClause();
            }

            if (mTopRecords > 0)
            {
                switch (mDbConfig.Provider)
                {
                    case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                        {
                            query = query + " FETCH FIRST " + mTopRecords + " ROWS ONLY ";
                            break;
                        }
                    case Providers.OracleDataAccess:

                        if (Convert.ToInt32(this.DbConfig.OracleVersion) >= 12)
                        {
                            query = query + " FETCH FIRST " + this.mTopRecords + " ROWS ONLY ";
                        }
                        else
                        {
                            query = "SELECT * FROM (" + query + ") WHERE ROWNUM <= " + this.mTopRecords;
                        }

                        break;


                    default:
                        {
                            break;
                        }
                }
            }

            if (mDbObjectType == DbObjectTypeEnum.Join)
            {
                if (mDbConfig.SupportSquareBrackets == true)
                {
                    query = query.Replace("[", "");
                    query = query.Replace("]", "");
                }
            }

            SQLSelect = query;
            if (DbConnection.State == System.Data.ConnectionState.Closed)
            {
                DbConnection.Open();
            }

            switch (mDbConfig.Provider)
            {
                case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                    {
                        if (objCommand.Transaction is null)
                        {
                            objCommand.Transaction = DbConfig.DbTransaction;
                        }

                        break;
                    }

                default:
                    {
                        if (objCommand.Transaction is null)
                        {
                            objCommand.Transaction = DbConfig.DbTransaction;
                        }

                        break;
                    }
            }

            objCommand.Connection = DbConnection;
            objCommand.CommandTimeout = CommandTimeOut;
            var argAdapter = objAdapter;
            objCommandBuilder = SetupCommandBuilder(argAdapter);
            objAdapter = argAdapter;
            objAdapter.SelectCommand.CommandTimeout = CommandTimeOut;


            switch (mDbConfig.Provider)
            {
                case Providers.OracleDataAccess:

                    objCommand.CommandText = query + " " + AppendedSQL;
                    objCommand.CommandType = commandtype;
                    break;
                default:
                    {
                        {
                            objCommand.CommandText = query + " " + AppendedSQL;
                            objCommand.CommandType = commandtype;
                        }
                        break;
                    }
            }

            try
            {
                // PUNTOCRITICO
                mRowCount = objAdapter.Fill(ds, DbTableName);
                ds.Tables[0].TableName = DbTableName;
            }
            catch (Exception ex)
            {
                HandleExceptions(ex);
            }
            finally
            {
            }

            query = "";


            if (mDbConfig.DbConnectionKeepOpen == false)
                DbConnection.Close();

            return ds;
        }

        #endregion

        #region AddToDataSet Method
        public int AddtoDataSet(ref DataSet DataSet, string query, CommandType commandtype, ConnectionState connectionstate)
        {
            ExecutionResult.Context = "DbObject:AddToDataSet";
            if (mIsReadOnly == true)
                return default;
            bool Cancel = false;
            if (DisableEvents == false)
            {
                DataEventBefore?.Invoke(DataEventType.AddToDataSet, ref Cancel);
            }

            if (Cancel == true)
                return default;
            var adapter = objFactory.CreateDataAdapter();
            string sSELECT = "SELECT ";
            if (mDistinctClause == true)
            {
                sSELECT = "SELECT DISTINCT ";
            }

            string Query1 = sSELECT + " * FROM " + DbTableName;

            if (query.Trim() == "")
            {
                objFiltersGroup.ParametersStyle = mDbConfig.ParametersStyle;
                DbCommand argDBCommand = objCommand;
                objFiltersGroup.BuildSQLFilter(ref query, ref argDBCommand, mDbConfig.Provider);
                objCommand = (DbCommand)argDBCommand;
                if (!string.IsNullOrEmpty(query))
                {
                    query = Query1 + " WHERE " + query;
                }
                else
                {
                    query = Query1;
                }
            }

            if (DbConnection.State == System.Data.ConnectionState.Closed)
            {
                DbConnection.Open();
            }

            objCommand.Connection = DbConnection;
            objCommand.CommandText = query;
            objCommand.CommandType = commandtype;
            adapter.SelectCommand = objCommand;
            try
            {
                adapter.Fill(DataSet, DbTableName);
            }
            catch (Exception ex)
            {
                HandleExceptions(ex);
            }
            finally
            {
                objCommand.Parameters.Clear();
                if (connectionstate == ConnectionState.CloseOnExit)
                {
                    // If objConnection.State = System.Data.ConnectionState.Open Then
                    // objConnection.Close()
                    // End If
                    if (DbConnection.State == System.Data.ConnectionState.Open)
                    {
                        DbConnection.Close();
                    }
                }
            }

            if (DisableEvents == false)
            {
                DataEventAfter?.Invoke(DataEventType.AddToDataSet);
            }

            return default;
        }
        #endregion

        public int UpdateFromDataSet(ref DataSet DataSet)
        {
            ExecutionResult.Context = "DbObject:UpdateFromDataSet_1";
            if (mIsReadOnly == true)
                return default;
            bool Cancel = false;
            if (DisableEvents == false)
            {
                DataEventBefore?.Invoke(DataEventType.UpdateFromDataSet, ref Cancel);
            }

            if (Cancel == true)
                return default;

            objCommandBuilder.RefreshSchema();

            if (objCommandBuilder.DataAdapter is null)
            {
                SetupCommandBuilder();
            
            }
            
            objCommandBuilder.RefreshSchema();
            objCommandBuilder = SetupCommandBuilder(this.objAdapter);

            //SetupUpdateCommand(ref objUpdateCommand);
            //SetupDeleteCommand(ref objDeleteCommand);
            //SetupInsertCommand(ref objInsertCommand);
            if (objAdapter.SelectCommand is object)
            {
                if (objAdapter.SelectCommand.Transaction is null & DbConfig.DbTransaction is object)
                {
                    switch (mDbConfig.Provider)
                    {
                        case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                            {
                                objAdapter.SelectCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }

                        default:
                            {
                                objAdapter.SelectCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }
                    }
                }
            }

            if (objAdapter.UpdateCommand is object)
            {
                if (objAdapter.UpdateCommand.Transaction is null & DbConfig.DbTransaction is object)
                {
                    switch (mDbConfig.Provider)
                    {
                        case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                            {
                                objAdapter.UpdateCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }

                        default:
                            {
                                objAdapter.UpdateCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }
                    }
                }
            }

            if (objAdapter.InsertCommand is object)
            {
                if (objAdapter.InsertCommand.Transaction is null & DbConfig.DbTransaction is object)
                {
                    switch (mDbConfig.Provider)
                    {
                        case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                            {
                                objAdapter.InsertCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }

                        default:
                            {
                                objAdapter.InsertCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }
                    }
                }
            }

            if (objAdapter.DeleteCommand is object)
            {
                if (objAdapter.DeleteCommand.Transaction is null & DbConfig.DbTransaction is object)
                {
                    switch (mDbConfig.Provider)
                    {
                        case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                            {
                                objAdapter.DeleteCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }

                        default:
                            {
                                objAdapter.DeleteCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }
                    }
                }
            }

            //SetupUpdateCommand(ref objUpdateCommand);
            //SetupDeleteCommand(ref objDeleteCommand);
            //SetupInsertCommand(ref objInsertCommand);

            try
            {
                objAdapter.Update(DataSet, DataSet.Tables[0].TableName);
            }
            catch (Exception ex)
            {
                HandleExceptions(ex);
            }

            if (DisableEvents == false)
            {
                DataEventAfter?.Invoke(DataEventType.UpdateFromDataSet);
            }

            return default;
        }
        public void SetupCommands()
        {
            SetupUpdateCommand(ref objUpdateCommand);
            SetupDeleteCommand(ref objDeleteCommand);
            SetupInsertCommand(ref objInsertCommand);
        }
        public int UpdateFromDataSet()
        {
            ExecutionResult.Context = "DbObject:ExecuteDataSet_2";
            if (mIsReadOnly == true)
                return default;
            bool Cancel = false;
            // If Me.DisableEvents = False Then
            DataEventBefore?.Invoke(DataEventType.UpdateFromDataSet, ref Cancel);
            // End If

            if (Cancel == true)
                return default;
            int i;
            //DbDataAdapter argAdapter = objAdapter;
            //objCommandBuilder = SetupCommandBuilder(argAdapter);
            //objAdapter = argAdapter;
           
            objCommandBuilder = SetupCommandBuilder(objAdapter);
            objCommandBuilder.RefreshSchema();
            //SetupUpdateCommand(ref objUpdateCommand);
            //SetupDeleteCommand(ref objDeleteCommand);
            //SetupInsertCommand(ref objInsertCommand);

            if (objAdapter.SelectCommand is object)
            {
                if (objAdapter.SelectCommand.Transaction is null & DbConfig.DbTransaction is object)
                {
                    switch (mDbConfig.Provider)
                    {
                        case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                            {
                                objAdapter.SelectCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }

                        default:
                            {
                                objAdapter.SelectCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }
                    }
                }
            }

            if (objAdapter.UpdateCommand is object)
            {
                if (objAdapter.UpdateCommand.Transaction is null & DbConfig.DbTransaction is object)
                {
                    switch (mDbConfig.Provider)
                    {
                        case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                            {
                                objAdapter.UpdateCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }

                        default:
                            {
                                objAdapter.UpdateCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }
                    }
                }
            }

            if (objAdapter.InsertCommand is object)
            {
                if (objAdapter.InsertCommand.Transaction is null & DbConfig.DbTransaction is object)
                {
                    switch (mDbConfig.Provider)
                    {
                        case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                            {
                                objAdapter.InsertCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }

                        default:
                            {
                                objAdapter.InsertCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }
                    }
                }
            }

            if (objAdapter.DeleteCommand is object)
            {
                if (objAdapter.DeleteCommand.Transaction is null & DbConfig.DbTransaction is object)
                {
                    switch (mDbConfig.Provider)
                    {
                        case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                            {
                                objAdapter.DeleteCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }

                        default:
                            {
                                objAdapter.DeleteCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }
                    }
                }
            }

            //SetupUpdateCommand(ref objUpdateCommand);
            //SetupDeleteCommand(ref objDeleteCommand);
            //SetupInsertCommand(ref objInsertCommand);

            try
            {
                i = objAdapter.Update(objDataSet, objDataSet.Tables[0].TableName);
            }
            catch (Exception ex)
            {
                HandleExceptions(ex);
            }

            if (DisableEvents == false)
            {
                DataEventAfter?.Invoke(DataEventType.UpdateFromDataSet);
            }

            return default;
        }

        public int DeleteFromDataTable()
        {
            ExecutionResult.Context = "DbObject:DeleteFromDataTable_1";
            return UpdateFromDataTable();
        }

        public int DeleteFromDataTable(ref DataTable DataTable)
        {
            ExecutionResult.Context = "DbObject:DeleteFromDataTable_2";
            return UpdateFromDataTable(ref DataTable);
        }

        public int UpdateFromDataTableOLD(ref DataTable DataTable)
        {
            ExecutionResult.Context = "DbObject:UpdateFromDataTable";
            bool Cancel = false;
            if (DisableEvents == false)
            {
                DataEventBefore?.Invoke(DataEventType.UpdateFromDataTable, ref Cancel);
            }

            if (Cancel == true)
                return default;
            int AffectedRecords = 0;
            //if (objCommandBuilder.DataAdapter is null)
            //{
            //    SetupCommandBuilder();
            //}


            objCommandBuilder.RefreshSchema();
            objCommandBuilder = SetupCommandBuilder(objAdapter);
            objCommandBuilder.DataAdapter = objAdapter;

            //this.objInsertCommand.Transaction = this.DbConfig.DbTransaction;
            //this.objDeleteCommand.Transaction = this.DbConfig.DbTransaction;
            //this.objUpdateCommand.Transaction = this.DbConfig.DbTransaction;
            //this.objCommand.Transaction = this.DbConfig.DbTransaction;

            //SetupUpdateCommand(ref objUpdateCommand);
            //SetupDeleteCommand(ref objDeleteCommand);
            //SetupInsertCommand(ref objInsertCommand);
            if (objAdapter.SelectCommand is object)
            {
                if (objAdapter.SelectCommand.Transaction is null & DbConfig.DbTransaction is object)
                {
                    switch (mDbConfig.Provider)
                    {
                        case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                            {
                                objAdapter.SelectCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }

                        default:
                            {
                                objAdapter.SelectCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }
                    }
                }
            }

            if (objAdapter.UpdateCommand is object)
            {
                if (objAdapter.UpdateCommand.Transaction is null & DbConfig.DbTransaction is object)
                {
                    switch (mDbConfig.Provider)
                    {
                        case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                            {
                                objAdapter.UpdateCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }

                        default:
                            {
                                objAdapter.UpdateCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }
                    }
                }
            }

            if (objAdapter.InsertCommand is object)
            {
                if (objAdapter.InsertCommand.Transaction is null & DbConfig.DbTransaction is object)
                {
                    switch (mDbConfig.Provider)
                    {
                        case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                            {
                                objAdapter.InsertCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }

                        default:
                            {
                                objAdapter.InsertCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }
                    }
                }
            }

            if (objAdapter.DeleteCommand is object)
            {
                if (objAdapter.DeleteCommand.Transaction is null & DbConfig.DbTransaction is object)
                {
                    switch (mDbConfig.Provider)
                    {
                        case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                            {
                                objAdapter.DeleteCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }

                        default:
                            {
                                objAdapter.DeleteCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }
                    }
                }
            }

            //SetupUpdateCommand(ref objUpdateCommand);
            //SetupDeleteCommand(ref objDeleteCommand);
            //SetupInsertCommand(ref objInsertCommand);

            try
            {
                switch (mDbConfig.Provider)
                {
                    case Providers.ODBC or Providers.ODBC_DB2:
                        {
                            objAdapter.UpdateBatchSize = this.BatchRows;
                            break;
                        }

                    default:
                        {
                            objAdapter.UpdateBatchSize = this.BatchRows;
                            break;
                        }
                }

                int oldCurrentPosition = (int)mCurrentPosition;
                AffectedRecords = objAdapter.Update(DataTable);
                DoQuery();


            }
            catch (Exception ex)
            {
                HandleExceptions(ex);
            }

            if (DisableEvents == false)
            {
                DataEventAfter?.Invoke(DataEventType.UpdateFromDataTable);
            }

            return AffectedRecords;
        }

        public int UpdateFromDataTable(ref DataTable DataTable)
        {
            ExecutionResult.Context = "DbObject:UpdateFromDataTable";
            bool Cancel = false;
            if (DisableEvents == false)
            {
                DataEventBefore?.Invoke(DataEventType.UpdateFromDataTable, ref Cancel);
            }

            if (Cancel == true)
                return default;
            int AffectedRecords = 0;
            
            
            if (objAdapter.SelectCommand is object)
            {
                if (objAdapter.SelectCommand.Transaction is null & DbConfig.DbTransaction is object)
                {
                    switch (mDbConfig.Provider)
                    {
                        case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                            {
                                objAdapter.SelectCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }

                        default:
                            {
                                objAdapter.SelectCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }
                    }
                }
            }

            if (objAdapter.UpdateCommand is object)
            {
                if (objAdapter.UpdateCommand.Transaction is null & DbConfig.DbTransaction is object)
                {
                    switch (mDbConfig.Provider)
                    {
                        case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                            {
                                objAdapter.UpdateCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }

                        default:
                            {
                                objAdapter.UpdateCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }
                    }
                }
            }

            if (objAdapter.InsertCommand is object)
            {
                if (objAdapter.InsertCommand.Transaction is null & DbConfig.DbTransaction is object)
                {
                    switch (mDbConfig.Provider)
                    {
                        case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                            {
                                objAdapter.InsertCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }

                        default:
                            {
                                objAdapter.InsertCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }
                    }
                }
            }

            if (objAdapter.DeleteCommand is object)
            {
                if (objAdapter.DeleteCommand.Transaction is null & DbConfig.DbTransaction is object)
                {
                    switch (mDbConfig.Provider)
                    {
                        case Providers.ODBC_DB2 or Providers.OleDb_DB2:
                            {
                                objAdapter.DeleteCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }

                        default:
                            {
                                objAdapter.DeleteCommand.Transaction = DbConfig.DbTransaction;
                                break;
                            }
                    }
                }
            }

            //SetupUpdateCommand(ref objUpdateCommand);
            //SetupDeleteCommand(ref objDeleteCommand);
            //SetupInsertCommand(ref objInsertCommand);

            try
            {
                //switch (mDbConfig.Provider)
                //{
                //    case Providers.ODBC or Providers.ODBC_DB2:
                //        {
                //            //OdbcDataAdapter OdbcDataAdapter = (OdbcDataAdapter)objAdapter;
                //            //OdbcDataAdapter.UpdateBatchSize = 100;
                //            //objAdapter.UpdateBatchSize = 0; //this.BatchRows;
                //            break;
                //        }

                //    case Providers.OleDb_DB2:
                //        {
                //            //OleDbDataAdapter oleDbDataAdapter = (OleDbDataAdapter)objAdapter;
                //            //oleDbDataAdapter.UpdateBatchSize = 100;
                //            //objAdapter.UpdateBatchSize = 0; // this.BatchRows;
                //            break;
                //        }
                //    case Providers.DB2_iSeries:
                //        {
                //            //IBM .Data .DB2 .iSeries .iDB2DataAdapter dB2DataAdapter  = (IBM.Data.DB2.iSeries.iDB2DataAdapter)objAdapter;
                //            //dB2DataAdapter.UpdateBatchSize = 100;
                //            //objAdapter.UpdateBatchSize = 0; //this.BatchRows;
                //            break;
                //        }

                //    case Providers.MySQL:
                //        break;
                //    default:
                //        {
                //            //objAdapter.UpdateBatchSize = 0;  //this.BatchRows;
                //            break;
                //        }
                //}

                int oldCurrentPosition = (int)mCurrentPosition;
                AffectedRecords = objAdapter.Update(this.DataTable);
                DoQuery();


            }
            catch (Exception ex)
            {
                HandleExceptions(ex);
            }

            if (DisableEvents == false)
            {
                DataEventAfter?.Invoke(DataEventType.UpdateFromDataTable);
            }

            return AffectedRecords;
        }





        public int UpdateBatch()
        {
            ExecutionResult.Context = "DbObject:UpdateBatch";
            DataTable argDataTable = objDataTable ;
            return UpdateFromDataTable(ref argDataTable);

        }

        public int UpdateFromDataTable()
        {
            ExecutionResult.Context = "DbObject:UpdateFromDataTable_3";
            DataTable argDataTable = objDataTable ;
            return UpdateFromDataTable(ref argDataTable);

        }

        public int DeleteFromDataSet(ref DataSet DataSet)
        {
            ExecutionResult.Context = "DbObject:DeleteFromDataSet";
            bool Cancel = false;
            if (DisableEvents == false)
            {
                DataEventBefore?.Invoke(DataEventType.DeleteFromDataSet, ref Cancel);
            }

            if (Cancel == true)
                return default;
            SetupCommandBuilder();
            try
            {
                objAdapter.Update(DataSet, DataSet.Tables[0].TableName);
            }
            catch (Exception ex)
            {
                HandleExceptions(ex);
            }

            if (DisableEvents == false)
            {
                DataEventAfter?.Invoke(DataEventType.DeleteFromDataSet);
            }

            return default;
        }

        private DbCommandBuilder SetupCommandBuilder(DbDataAdapter Adapter)
        {
            ExecutionResult.Context = "DbObject:SetupCommandBuilder_1";
            switch (mDbConfig.Provider)
            {
                case Providers.ConfigDefined:
                    {
                        return new SqlCommandBuilder((SqlDataAdapter)Adapter);
                    }

                case Providers.ODBC or Providers.ODBC_DB2:
                    {
                        return new OdbcCommandBuilder((OdbcDataAdapter)Adapter);
                    }

                case Providers.OleDb or Providers.OleDb_DB2:
                    {
                        return new OleDbCommandBuilder((OleDbDataAdapter)Adapter);
                    }

                case Providers.OracleDataAccess:
#if COMPILEORACLE
                    return new Oracle.ManagedDataAccess.Client.OracleCommandBuilder((Oracle.ManagedDataAccess.Client.OracleDataAdapter)Adapter);
#endif

                case Providers.SqlServer:
                    {
                        return new SqlCommandBuilder((SqlDataAdapter)Adapter);
                    }
                case Providers.DB2_iSeries :
                    {
                        return new IBM.Data .DB2 .iSeries .iDB2CommandBuilder((IBM.Data .DB2 .iSeries.iDB2DataAdapter)Adapter);
                    }

                case Providers.MySQL:
                    {
                        return new MySql.Data.MySqlClient.MySqlCommandBuilder((MySql.Data.MySqlClient.MySqlDataAdapter)Adapter);
                    }

                default:
                    {
                        return new SqlCommandBuilder((SqlDataAdapter)Adapter);
                    }
            }
        }

        private void SetupCommandBuilder()
        {
            ExecutionResult.Context = "DbObject:SetupCommandBuilder_2";
            switch (mDbConfig.Provider)
            {
                case Providers.ConfigDefined:
                    {
                        objCommandBuilder = new SqlCommandBuilder((SqlDataAdapter)objAdapter);
                        break;
                    }

                case Providers.ODBC or Providers.ODBC_DB2:
                    {
                        objCommandBuilder = new OdbcCommandBuilder((OdbcDataAdapter)objAdapter);
                        break;
                    }

                case Providers.OleDb or Providers.OleDb_DB2:
                    {
                        objCommandBuilder = new OleDbCommandBuilder((OleDbDataAdapter)objAdapter);
                        break;
                    }

                case Providers.OracleDataAccess:

#if COMPILEORACLE
                    this.objCommandBuilder = new Oracle.ManagedDataAccess.Client.OracleCommandBuilder((Oracle.ManagedDataAccess.Client.OracleDataAdapter)this.objAdapter);
#endif
                    break;

                case Providers.SqlServer:
                    {
                        objCommandBuilder = new SqlCommandBuilder((SqlDataAdapter)objAdapter);
                        break;
                    }

                case Providers.DB2_iSeries :
                    {
                        objCommandBuilder = new IBM.Data.DB2.iSeries.iDB2CommandBuilder((IBM.Data.DB2.iSeries.iDB2DataAdapter)objAdapter);
                        break;
                    }
                case Providers.MySQL :
                    {
                        objCommandBuilder = new MySql.Data.MySqlClient .MySqlCommandBuilder((MySql .Data .MySqlClient .MySqlDataAdapter)objAdapter);
                        break;
                    }
            }
        }

        public void Dispose()
        {
            Dispose1();
        }

        public void Dispose1()
        {
            ExecutionResult.Context = "DbObject:Dispose1";
            try
            {
                if (objDataReader is object)
                {
                    if (objDataReader.IsClosed == false)
                    {
                        objDataReaderCommand.Cancel();
                        objDataReader.Close();
                    }

                    objDataReader.Dispose();
                    objDataReaderCommand.Dispose();
                    objDataReaderConnection.Close();
                    objDataReaderConnection.Dispose();
                }

                if (objAdapter is object)
                    objAdapter.Dispose();
                if (objDataTable is object)
                    objDataTable.Dispose();
                if (objDataSet is object)
                    objDataSet.Dispose();
                if (objCommand is object)
                    objCommand.Dispose();
            }
            catch (Exception ex)
            {
            }
        }

        void IDisposable.Dispose() => Dispose1();

        private Join[] GetJoins(object ClassObj)
        {
            Join[] GetJoinsRet = default;
            ExecutionResult.Context = "DbObject:GetJoins";
            int i = 0;
            var lJoins = new Join[1];
            var index = default(int);
            DbJoin dbJoin;
            FieldInfo xfi;
            switch (InterfaceMode)
            {
                case InterfaceModeEnum.Property:
                    {
                        mProperties = rGetProperties(this);
                        lJoins = new Join[mProperties.Length + 1];
                        foreach (var currentMProperty in mProperties)
                        {
                            mProperty = currentMProperty;
                            if (mProperty.PropertyType.Name.ToUpper() == "DBJOIN")
                            {

                                dbJoin = (DbJoin)Interaction.CallByName(this, mProperty.Name, CallType.Get);
                                lJoins[i].Name = mProperty.Name;
                                lJoins[i].DbJoin = dbJoin;
                                i = i + 1;
                            }

                            index = index + 1;

                        }

                        break;
                    }

                case InterfaceModeEnum.Public or InterfaceModeEnum.Private:
                    {
                        mFields = rGetFields(this);
                        lJoins = new Join[mFields.Length + 1];
                        foreach (var currentMField in mFields)
                        {
                            mField = currentMField;
                            if (mField.FieldType.Name.ToUpper() == "DBJOIN")
                            {
                                if (InterfaceMode == InterfaceModeEnum.Private)
                                {
                                    xfi = GetType().GetField(mField.Name, BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Public | BindingFlags.Static);
                                    dbJoin = (DbJoin)xfi.GetValue(this);
                                }
                                else
                                {
                                    //dbJoin = (DbJoin)Utilities.CallByName(this, mField.Name, Utilities.CallType.Get);
                                    dbJoin = (DbJoin)Interaction.CallByName(this, mField.Name, CallType.Get);
                                }

                                lJoins[i].Name = mField.Name;
                                lJoins[i].DbJoin = dbJoin;

                                i = i + 1;
                            }
                            index = index + 1;
                        }

                        break;
                    }
            }

            if (i > 1)
            {
                Array.Resize(ref lJoins, i);
            }

            GetJoinsRet = lJoins;
            return GetJoinsRet;
        }

        private Field[] GetFields(object ClassObj)
        {
            Field[] GetFieldsRet = default;
            ExecutionResult.Context = "DbObject:GetFields";
            int i = 0;
            Field[] lFields = new Field[1];
            int index = 0;
            DbColumn dbcolumn;
            FieldInfo xfi;
            string cAlias = "";
            var s_FieldList = new StringBuilder();
            FieldsList = "";
            switch (InterfaceMode)
            {
                case InterfaceModeEnum.Property:
                    {
                        mProperties = rGetProperties(this);
                        lFields = new Field[mProperties.Length + 1];
                        foreach (var currentMProperty in mProperties)
                        {
                            mProperty = currentMProperty;
                            if (mProperty.PropertyType.Name.ToUpper() == "DBCOLUMN")
                            {
                                dbcolumn = (DbColumn)Interaction.CallByName(this, mProperty.Name, CallType.Get);
                                switch (mDbConfig.Provider)
                                {
                                    case Providers.OracleDataAccess:
                                        dbcolumn.DbColumnName = dbcolumn.DbColumnName.ToUpper();
                                        dbcolumn.DbColumnNameAlias = dbcolumn.DbColumnNameAlias.ToUpper();
                                        dbcolumn.DbColumnNameE = dbcolumn.DbColumnNameE.ToUpper();
                                        break;
                                    default:
                                        break;
                                }

                                lFields[i].Name = mProperty.Name;
                                lFields[i].Type = mProperty.PropertyType.GetType();
                                lFields[i].TypeName = mProperty.PropertyType.Name.ToUpper();
                                lFields[i].Index = index;
                                lFields[i].DbColumnName = dbcolumn.DbColumnName;
                                lFields[i].DbColumnNameE = dbcolumn.DbColumnNameE;
                                lFields[i].DbColumnNameAlias = dbcolumn.DbColumnNameAlias;
                                

                                if (!string.IsNullOrEmpty(dbcolumn.DbColumnNameAlias))
                                {
                                    cAlias = " as " + dbcolumn.DbColumnNameAlias;
                                }
                                else
                                {
                                    cAlias = "";
                                }


                                if (dbcolumn.Extraction == true)
                                {
                                    s_FieldList.Append(dbcolumn.DbColumnName + cAlias + ",");
                                }
                                else
                                {
                                    // MsgBox("no")
                                }

                                i = i + 1;
                            }

                            index = index + 1;
                        }

                        break;
                    }

                case InterfaceModeEnum.Public or InterfaceModeEnum.Private:
                    {
                        mFields = rGetFields(this);
                        lFields = new Field[mFields.Length + 1];
                        //foreach (var currentMField in mFields)
                        foreach (FieldInfo mField in mFields)
                        {
                            if (mField.FieldType.Name.ToUpper() == "DBCOLUMN")
                            {
                                if (InterfaceMode == InterfaceModeEnum.Private)
                                {
                                    xfi = GetType().GetField(mField.Name, BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Public | BindingFlags.Static);
                                    dbcolumn = (DbColumn)xfi.GetValue(this);
                                }
                                else
                                {

                                    xfi = GetType().GetField(mField.Name, BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Public | BindingFlags.Static);
                                    dbcolumn = (DbColumn)xfi.GetValue(this);
                                    // xfi = Me.GetType().GetField(mField.Name, BindingFlags.Public)
                                    // dbcolumn = xfi.GetValue(Me)

                                    // dbcolumn = CallByName(Me, mField.Name, CallType.Get)
                                    //dbcolumn = (DbColumn)Utilities.CallByName(this, mField.Name,Utilities.CallType.Get);
                                }

                                switch (mDbConfig.Provider)
                                {
                                    case Providers.OracleDataAccess:
                                        dbcolumn.DbColumnName = dbcolumn.DbColumnName.ToUpper();
                                        dbcolumn.DbColumnNameAlias = dbcolumn.DbColumnNameAlias.ToUpper();
                                        dbcolumn.DbColumnNameE = dbcolumn.DbColumnNameE.ToUpper();
                                        break;
                                    default:
                                        break;
                                }



                                lFields[i].Name = mField.Name;
                                lFields[i].Type = mField.FieldType.GetType();
                                lFields[i].TypeName = mField.FieldType.Name;
                                lFields[i].Index = index;
                                lFields[i].DbColumnName = dbcolumn.DbColumnName;
                                lFields[i].DbColumnNameAlias = dbcolumn.DbColumnNameAlias;
                                lFields[i].DbColumnNameE = dbcolumn.DbColumnNameE;
                              
                                if (!string.IsNullOrEmpty(dbcolumn.DbColumnNameAlias))
                                {
                                    cAlias = " as " + dbcolumn.DbColumnNameAlias;
                                }
                                else
                                {
                                    cAlias = "";
                                }

                                if (dbcolumn.Extraction == true)
                                {
                                    s_FieldList.Append(dbcolumn.DbColumnName + cAlias + ",");
                                }
                                //else
                                //{
                                //   // cAlias = cAlias;
                                //}

                                i = i + 1;
                            }

                            index = index + 1;
                        }

                        break;
                    }
            }

            if (i > 1)
            {
                Array.Resize(ref lFields, i);
            }

            GetFieldsRet = lFields;
            FieldsList = s_FieldList.ToString();
            if (i > 0)
                //FieldsList = Strings.Mid(FieldsList, 1, Strings.Len(FieldsList) - 1);
                FieldsList = FieldsList.Substring(0, FieldsList.Length - 1);
            return GetFieldsRet;
        }

        private Parameter[] GetParameters(object ClassObj)
        {
            Parameter[] GetParametersRet = default;
            ExecutionResult.Context = "DbObject:GetParameters";
            int i = 0;
            var lParameter = new Parameter[1];
            var index = default(int);
            DbParameter DbParameter;
            FieldInfo xfi;
            string cAlias = "";
            var sbParameterList = new StringBuilder();
            ParameterList = "";
            switch (InterfaceMode)
            {
                case InterfaceModeEnum.Property:
                    {
                        mProperties = rGetProperties(this);
                        lParameter = new Parameter[mProperties.Length + 1];
                        foreach (PropertyInfo mProperty in mProperties)
                        {

                            if (mProperty.PropertyType.Name.ToUpper() == "DBPARAMETER")
                            {
                                DbParameter = (DbParameter)Interaction.CallByName(this, mProperty.Name, CallType.Get);
                                lParameter[i].Name = mProperty.Name;
                                lParameter[i].Type = mProperty.PropertyType.GetType();
                                lParameter[i].TypeName = mProperty.PropertyType.Name.ToUpper();
                                lParameter[i].Index = index;
                                lParameter[i].ParameterName = DbParameter.ParameterName;
                                i = i + 1;
                            }

                            index = index + 1;
                        }

                        break;
                    }

                case InterfaceModeEnum.Public or InterfaceModeEnum.Private:
                    {
                        mFields = rGetFields(this);
                        lParameter = new Parameter[mFields.Length];
                        foreach (FieldInfo mField in mFields)
                        {

                            if (mField.FieldType.Name.ToUpper() == "DBPARAMETER")
                            {
                                if (InterfaceMode == InterfaceModeEnum.Private)
                                {
                                    xfi = GetType().GetField(mField.Name, BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Public | BindingFlags.Static);
                                    DbParameter = (DbParameter)xfi.GetValue(this);
                                }
                                else
                                {

                                    // xfi = Me.GetType().GetField(mField.Name, BindingFlags.Public)
                                    // dbcolumn = xfi.GetValue(Me)

                                    // dbcolumn = CallByName(Me, mField.Name, CallType.Get)
                                    DbParameter = (DbParameter)Interaction.CallByName(this, mField.Name, CallType.Get);
                                }

                                lParameter[i].Name = mField.Name;
                                lParameter[i].Type = mField.FieldType.GetType();
                                lParameter[i].TypeName = mField.FieldType.Name;
                                lParameter[i].Index = index;
                                lParameter[i].ParameterName = DbParameter.ParameterName;
                                i = i + 1;
                            }

                            index = index + 1;
                        }

                        break;
                    }
            }

            if (i > 0)
            {
                if (i > 1)
                {
                    Array.Resize(ref lParameter, i);
                }
            }

            GetParametersRet = lParameter;
            return GetParametersRet;
        }

        private FieldInfo[] rGetMembers(object ClassObj)
        {
            ExecutionResult.Context = "DbObject:rGetMembers";
            MemberInfo[] fi;
            var ty = ClassObj.GetType();
            fi = ty.GetFields(BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Public | BindingFlags.Static);
            return (FieldInfo[])fi;
        }

        private FieldInfo[] rGetFields(object ClassObj)
        {
            ExecutionResult.Context = "DbObject:rGetFields";
            FieldInfo[] fi;
            var ty = ClassObj.GetType();
            fi = ty.GetFields(BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Public | BindingFlags.Static);
            return fi;
        }

        private PropertyInfo[] rGetProperties(object ClassObj)
        {
            ExecutionResult.Context = "DbObject:rGetProperties";
            PropertyInfo[] fi;
            var ty = ClassObj.GetType();
            fi = ty.GetProperties(BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Public | BindingFlags.Static);
            return fi;
        }

        private string GetClassName(object ClassObj)
        {
            string GetClassNameRet = default;
            ExecutionResult.Context = "DbObject:GetClassName";
            GetClassNameRet = ClassObj.GetType().Name;
            return GetClassNameRet;
        }

        private void BuildInMemoryJoinDataTable()
        {
            ExecutionResult.Context = "DbObject:BuildInMemoryJoinDataTable";
            var DbColumnsList = new ArrayList();
            DbColumns DbColumns;
            DbColumn DBColumn;
            foreach (DbObject DbObject in DbObjects)
            {
                DbColumns = DbObject.GetDbColumns();
                foreach (DbColumn currentDBColumn in DbColumns)
                {
                    DBColumn = currentDBColumn;
                    DBColumn.TableName = DbObject.DbTableName;
                    DbColumnsList.Add(DBColumn);

                }

            }

            mInMemoryJoinDataTable = new DataTable();
            DataColumn c;
            foreach (DbColumn currentDBColumn1 in DbColumnsList)
            {
                DBColumn = currentDBColumn1;
                c = new DataColumn(TabColName(DBColumn.TableName, DBColumn.DbColumnName));
                c.DataType = DbTypeToDataType(DBColumn.DbType);
                mInMemoryJoinDataTable.Columns.Add(c);
                // Threading.Thread.Sleep(0)
            }
        }

        private string TabColName(string TableName, string ColumnName)
        {
            ExecutionResult.Context = "DbObject:TabColName";
            return TableName.Replace(" ", "_").Trim("[]".ToCharArray()) + "_" + ColumnName.Replace(" ", "_").Trim("[]".ToCharArray());
        }
    }


    #endregion


    #region DbFilter Class
    public class DbFilter
    {
        private DbColumn mDbColumn;
        private ComparisionOperator mComparisionOperator;
        private object mValue;
        private LogicOperator mLogicOperator;
        private object mBoundControl;
        private string mPropertyName = "";
        private bool mUseBoundControl = false;

        public bool UseBoundControl
        {
            get
            {
                bool UseBoundControlRet = default;
                UseBoundControlRet = mUseBoundControl;
                return UseBoundControlRet;
            }

            set
            {
                mUseBoundControl = value;
            }
        }

        public string PropertyName
        {
            get
            {
                string PropertyNameRet = default;
                PropertyNameRet = mPropertyName;
                return PropertyNameRet;
            }

            set
            {
                mPropertyName = value;
            }
        }

        public object BoundControl
        {
            get
            {
                object BoundControlRet = default;
                BoundControlRet = mBoundControl;
                return BoundControlRet;
            }

            set
            {
                mBoundControl = value;
            }
        }

        public LogicOperator LogicOperator
        {
            get
            {
                LogicOperator LogicOperatorRet = default;
                LogicOperatorRet = mLogicOperator;
                return LogicOperatorRet;
            }

            set
            {
                mLogicOperator = value;
            }
        }

        public object Value
        {
            get
            {
                object ValueRet = default;
                ValueRet = mValue;
                return ValueRet;
            }

            set
            {
                mValue = value;
            }
        }

        public ComparisionOperator ComparisionOperator
        {
            get
            {
                ComparisionOperator ComparisionOperatorRet = default;
                ComparisionOperatorRet = mComparisionOperator;
                return ComparisionOperatorRet;
            }

            set
            {
                mComparisionOperator = value;
            }
        }

        public DbColumn DbColumn
        {
            get
            {
                DbColumn DbColumnRet = default;
                DbColumnRet = mDbColumn;
                return DbColumnRet;
            }

            set
            {
                mDbColumn = value;
            }
        }
    }
    #endregion

    #region DbFilters Class
    public class DbFilters : CollectionBase
    {
        private string mGroupName;
        private LogicOperator mLogicOperator;
        private int mOrdinalPosition;
        public object ExecutionResult = new ExecutionResult();
        private bool mUseBoundControl;

        public bool UseBoundControl
        {
            get
            {
                bool UseBoundControlRet = default;
                UseBoundControlRet = mUseBoundControl;
                return UseBoundControlRet;
            }

            set
            {
                mUseBoundControl = value;
            }
        }

        public DbFilter get_Item(int Index)
        {
            DbFilter ItemRet = default;
            try
            {
                ItemRet = (DbFilter)List[Index];
            }
            catch
            {
                return null;
            }

            return ItemRet;
        }

        public void set_Item(int Index, DbFilter value)
        {
            try
            {
                List[Index] = value;
            }
            catch
            {
            }
        }

        public int OrdinalPosition
        {
            get
            {
                int OrdinalPositionRet = default;
                OrdinalPositionRet = mOrdinalPosition;
                return OrdinalPositionRet;
            }

            set
            {
                mOrdinalPosition = value;
            }
        }

        public string GroupName
        {
            get
            {
                string GroupNameRet = default;
                GroupNameRet = mGroupName;
                return GroupNameRet;
            }

            set
            {
                mGroupName = value;
            }
        }

        public LogicOperator LogicOperator
        {
            get
            {
                LogicOperator LogicOperatorRet = default;
                LogicOperatorRet = mLogicOperator;
                return LogicOperatorRet;
            }

            set
            {
                mLogicOperator = value;
            }
        }

        public bool Add(DbFilter Filter)
        {
            try
            {
                List.Add(Filter);
                return true;
            }
            catch
            {
                return false;
            }
        }

        public bool Add(DbColumn DbColumn, ComparisionOperator ComparisionOperator, object Value, LogicOperator LogicOperator = LogicOperator.None)
        {
            var Filter = new DbFilter();
            Filter.DbColumn = DbColumn;
            Filter.LogicOperator = LogicOperator;
            Filter.ComparisionOperator = ComparisionOperator;
            Filter.Value = Value;
            try
            {
                List.Add(Filter);
                return true;
            }
            catch
            {
                return false;
            }
        }

        public bool AddBoundControl(DbColumn DbColumn, ComparisionOperator ComparisionOperator, object BoundControl, string PropertyName, LogicOperator LogicOperator = LogicOperator.None)
        {
            var Filter = new DbFilter();
            Filter.DbColumn = DbColumn;
            Filter.LogicOperator = LogicOperator;
            Filter.ComparisionOperator = ComparisionOperator;
            Filter.BoundControl = BoundControl;
            Filter.PropertyName = PropertyName;
            Filter.UseBoundControl = true;
            try
            {
                List.Add(Filter);
                return true;
            }
            catch
            {
                return false;
            }
        }
    }
    #endregion

    #region DbFiltersGroup Class
    public class DbFiltersGroup : ArrayList
    {
        public ParameterStyles ParametersStyle = ParameterStyles.Qualified;
        public bool Cache = false;
        public string CachedSQLWhereStmt = "";
        private string[] sLogicsOperators = new string[] { "", "AND", "OR", "NOT" };
        private string[] sComparisionOperators = new string[] { " = ", " > ", " < ", " >= ", " <= ", " <> ", " BETWEEN ", " IN ", " LIKE ", " NOT LIKE ", " EXISTS ", "", " IS NULL ", " IS NOT NULL ", " NOT IN ", " NOT BETWEEN " };
        public ExecutionResult ExectionResult;

        public DbFiltersGroup()
        {
        }

        public string SQLWhereStmt
        {
            get
            {
                string SQLWhereStmtRet = default;
                string SQL = "";
                BuildSQLFilter(ref SQL);
                SQLWhereStmtRet = SQL;
                return SQLWhereStmtRet;
            }
        }

        public void AddFilters(DbFilters Filters, LogicOperator LogicOperator = LogicOperator.None)
        {
            Filters.LogicOperator = LogicOperator;
            Add(Filters);
        }

        public bool BuildSQLFilter(ref string SQL)
        {
            DbCommand argDBCommand = null;
            return BuildSQLFilter(ref SQL, ref argDBCommand, Providers.SqlServer);
        }

        public bool BuildSQLDeleteFilter(ref DbObject DbObject, ref string SQL)
        {

            //bool localBuildSQLFilter() { object argDBCommand = DbObject.Command; var ret = BuildSQLFilter(ref SQL, ref argDBCommand, DbObject.DbProvider); DbObject.Command = (DbCommand)argDBCommand; return ret; }

            DbCommand DbCommand = DbObject.Command;
            if (BuildSQLFilter(ref SQL, ref DbCommand, DbObject.DbConfig.Provider) == true)
            {
                SQL = "DELETE FROM " + DbObject.DbTableName + " WHERE " + SQL;
                return true;
            }
            else
            {
                return false;
            }
        }

        public bool BuildSQLFilter(ref string SQL, ref DbCommand DbCommand, Providers Provider = Providers.SqlServer)
        {

            bool BuildSQLFilterRet = default;
            var s_SQL = new StringBuilder();
            string _SQL = "";
            DbFilter Filter = null;
            string ParameterName = "";
            int i = 0;
            object value;

            bool _Cache = false;
            int Index0 = 0;
            int Index1 = 0;


            if (this.Count == 0)
            {
                return true;
            }

            _Cache = this.Cache;

            DbCommand _DbCommand = DbCommand;

            if (_Cache == false)
            {
                if (_DbCommand != null)
                {
                    _DbCommand.Parameters.Clear();
                }
            }
            else if (_DbCommand != null)
            {
                if (_DbCommand.Parameters.Count == 0)
                {
                    _Cache = false;
                }
            }

            OdbcCommand _OdbcCommand = new OdbcCommand();
            OleDbCommand _OleDbCommand = new OleDbCommand();
            SqlCommand _SqlCommand = new SqlCommand();
            MySql.Data.MySqlClient.MySqlCommand _MySQLCommand= new MySql.Data.MySqlClient.MySqlCommand();
            IBM.Data.DB2.iSeries.iDB2Command _DB2iSeriesCommand = new IBM.Data.DB2.iSeries.iDB2Command();

#if COMPILEORACLE
            Oracle.ManagedDataAccess.Client.OracleCommand _OracleCommand = new Oracle.ManagedDataAccess.Client.OracleCommand();
#endif

            // FARE IL CAST DEL DbCommand ?


            switch (Provider)
            {
                case Providers.SqlServer:
                    _SqlCommand = (SqlCommand)DbCommand;
                    break;
                case Providers.DB2_iSeries :
                    _DB2iSeriesCommand = (IBM.Data .DB2 .iSeries .iDB2Command )DbCommand;
                    break;

                case Providers.MySQL:
                    _MySQLCommand = (MySql .Data .MySqlClient .MySqlCommand )DbCommand;
                    break;


                case Providers.OleDb:
                    _OleDbCommand = (OleDbCommand)DbCommand;
                    break;
                case Providers.OracleDataAccess:
#if COMPILEORACLE
                    _OracleCommand = (Oracle.ManagedDataAccess.Client.OracleCommand)DbCommand;
#endif
                    break;
                case Providers.ODBC:
                    _OdbcCommand = (OdbcCommand)DbCommand;
                    break;
                case Providers.ODBC_DB2:
                    _OdbcCommand = (OdbcCommand)DbCommand;
                    break;
                case Providers.OleDb_DB2:
                    _OleDbCommand = (OleDbCommand)DbCommand;
                    break;
                default:
                    break;
            }

            foreach (DbFilters Filters in this)
            {
                if (Index0 == this.Count - 1)
                    Filters.LogicOperator = LogicOperator.None;



                if (_Cache == false)
                {
                    // NO CACHE MUST CREATE PARAMETER
                    s_SQL.Append("(");
                    Index1 = 0;
                    foreach (DbFilter currentFilter in Filters)
                    {
                        Filter = currentFilter;
                        if (Index1 == Filters.Count - 1)
                            Filter.LogicOperator = LogicOperator.None;
                        value = null;
                        if (!string.IsNullOrEmpty(Filter.DbColumn.DbColumnName))
                        {
                            switch (Filter.ComparisionOperator)
                            {

                                case ComparisionOperator.Between or ComparisionOperator.NotBetween:
                                    {
                                        object _b1 = null;
                                        object _b2 = null;
                                        if (Filter.Value.GetType().IsArray == true)
                                        {
                                            dynamic _Filter = Filter;
                                            _b1 = _Filter.Value(0);
                                            _b2 = _Filter.Value(1);
                                        }
                                        if (Filter.Value.GetType().IsGenericType == true)
                                        {
                                            dynamic _Filter = Filter;
                                            _b1 = _Filter.Value(0);
                                            _b2 = _Filter.Value(1);
                                        }
                                        string Parameter1;
                                        string Parameter2;
                                        string xi = i.ToString().Trim();
                                        Parameter1 = Filter.DbColumn.DbObject.DbConfig.ParameterNamePrefixE + xi + "_" + xi + "_1"; // .DbColumn.DbObject.DbParameterNamePrefixE & xi & "_1"
                                        Parameter2 = Filter.DbColumn.DbObject.DbConfig.ParameterNamePrefixE + xi + "_" + xi + "_2"; // .DbColumn.DbObject.DbParameterNamePrefixE & xi & "_2"
                                        if (DbCommand != null)
                                        {
                                            if (Filter.UseBoundControl == false)
                                            {

                                                //var xparameter1 = new object();
                                                //var xparameter2 = new object();

                                                System.Data.Common.DbParameter xparameter1;
                                                System.Data.Common.DbParameter xparameter2;



                                                switch (Provider)
                                                {
                                                    case Providers.OracleDataAccess:
#if COMPILEORACLE
                                                        xparameter1 = _OracleCommand.Parameters.Add(Parameter1, _b1);
                                                        xparameter2 = _OracleCommand.Parameters.Add(Parameter2, _b2);
#endif
                                                        break;

                                                    case Providers.ODBC_DB2 or Providers.ODBC:
                                                        xparameter1 = _OdbcCommand.Parameters.AddWithValue(Parameter1, _b1);
                                                        xparameter2 = _OdbcCommand.Parameters.AddWithValue(Parameter2, _b2);
                                                        SetParameter(ref xparameter1, Filter.DbColumn);
                                                        SetParameter(ref xparameter2, Filter.DbColumn);
                                                        break;

                                                    case Providers.OleDb_DB2 or Providers.OleDb:
                                                        xparameter1 = _OleDbCommand.Parameters.AddWithValue(Parameter1, _b1);
                                                        xparameter2 = _OleDbCommand.Parameters.AddWithValue(Parameter2, _b2);
                                                        SetParameter(ref xparameter1, Filter.DbColumn);
                                                        SetParameter(ref xparameter2, Filter.DbColumn);
                                                        break;

                                                    case Providers.SqlServer:
                                                        xparameter1 = _SqlCommand.Parameters.AddWithValue(Parameter1, _b1);
                                                        xparameter2 = _SqlCommand.Parameters.AddWithValue(Parameter2, _b2);
                                                        break;
                                                    case Providers.DB2_iSeries :
                                                        xparameter1 = _DB2iSeriesCommand.Parameters.AddWithValue(Parameter1, _b1);
                                                        xparameter2 = _DB2iSeriesCommand.Parameters.AddWithValue(Parameter2, _b2);
                                                        break;
                                                    case Providers.MySQL :
                                                        xparameter1 = _MySQLCommand.Parameters.AddWithValue(Parameter1, _b1);
                                                        xparameter2 = _MySQLCommand.Parameters.AddWithValue(Parameter2, _b2);
                                                        break;



                                                    default:
                                                        break;
                                                }
                                            }
                                            else
                                            {
                                                value = Interaction.CallByName(Filter.BoundControl, Filter.PropertyName, CallType.Get);
                                                //var xparameter1 = new object();
                                                //var xparameter2 = new object();

                                                System.Data.Common.DbParameter xparameter1;
                                                System.Data.Common.DbParameter xparameter2;
                                                switch (Filter.DbColumn.DbObject.DbConfig.Provider)
                                                {

                                                    case Providers.OracleDataAccess:
#if COMPILEORACLE
                                                        xparameter1 = _OracleCommand.Parameters.Add(Parameter1, _b1);
                                                        xparameter2 = _OracleCommand.Parameters.Add(Parameter2, _b2);
#endif
                                                        break;

                                                    case Providers.ODBC_DB2 or Providers.ODBC:
                                                        xparameter1 = _OdbcCommand.Parameters.AddWithValue(Parameter1, value);
                                                        xparameter2 = _OdbcCommand.Parameters.AddWithValue(Parameter2, value);
                                                        SetParameter(ref xparameter1, Filter.DbColumn);
                                                        SetParameter(ref xparameter2, Filter.DbColumn);
                                                        break;

                                                    case Providers.OleDb_DB2 or Providers.OleDb:
                                                        xparameter1 = _OleDbCommand.Parameters.AddWithValue(Parameter1, value);
                                                        xparameter2 = _OleDbCommand.Parameters.AddWithValue(Parameter2, value);
                                                        SetParameter(ref xparameter1, Filter.DbColumn);
                                                        SetParameter(ref xparameter2, Filter.DbColumn);
                                                        break;
                                                    case Providers.SqlServer:
                                                        ;
                                                        xparameter1 = _SqlCommand.Parameters.AddWithValue(Parameter1, value);
                                                        xparameter2 = _SqlCommand.Parameters.AddWithValue(Parameter2, value);
                                                        break;
                                                    case Providers.DB2_iSeries :
                                                        
                                                        xparameter1 = _DB2iSeriesCommand.Parameters.AddWithValue(Parameter1, value);
                                                        xparameter2 = _DB2iSeriesCommand.Parameters.AddWithValue(Parameter2, value);
                                                        break;

                                                    case Providers.MySQL:
                                                        xparameter1 = _MySQLCommand.Parameters.AddWithValue(Parameter1, value);
                                                        xparameter2 = _MySQLCommand.Parameters.AddWithValue(Parameter2, value);
                                                        break;
                                                    default:
                                                        break;

                                                }
                                            }
                                        }

                                        switch (ParametersStyle)
                                        {
                                            case ParameterStyles.Qualified:
                                                {
                                                    s_SQL.Append(Filter.DbColumn.DbColumnName + sComparisionOperators[(int)Filter.ComparisionOperator] + Parameter1 + " AND " + Parameter2 + " " + sLogicsOperators[(int)Filter.LogicOperator] + " ");
                                                    break;
                                                }

                                            default:
                                                {
                                                    s_SQL.Append(Filter.DbColumn.DbColumnName + sComparisionOperators[(int)Filter.ComparisionOperator] + " ? AND ? " + sLogicsOperators[(int)Filter.LogicOperator] + " ");
                                                    break;
                                                }
                                        }

                                        break;
                                    }

                                case ComparisionOperator.IsNotNull or ComparisionOperator.IsNull:
                                    {
                                        s_SQL.Append(Filter.DbColumn.DbColumnName + sComparisionOperators[(int)Filter.ComparisionOperator] + " " + sLogicsOperators[(int)Filter.LogicOperator] + " ");
                                        break;
                                    }

                                case ComparisionOperator.In or ComparisionOperator.NotIn:
                                    {
                                        int _i = 1;
                                        StringBuilder _IN = new StringBuilder();
                                        string xi = "";
                                        foreach (var currentValue in (IEnumerable)Filter.Value)
                                        {
                                            value = currentValue;
                                            xi = _i.ToString().Trim();
                                            ParameterName = Filter.DbColumn.DbObject.DbConfig.ParameterNamePrefixE + i + "_" + xi;
                                            if (DbCommand != null)
                                            {
                                                if (Filter.UseBoundControl == false)
                                                {
                                                    //object xparameter;
                                                    System.Data.Common.DbParameter xparameter;
                                                    
                                                    switch (Provider)
                                                    {
                                                        case Providers.OracleDataAccess:
#if COMPILEORACLE
                                                            xparameter = _OracleCommand.Parameters.Add(ParameterName, value);
#endif
                                                            break;

                                                        case Providers.ODBC or Providers.ODBC_DB2:
                                                            xparameter = _OdbcCommand.Parameters.AddWithValue(ParameterName, value);
                                                            SetParameter(ref xparameter, Filter.DbColumn);
                                                            break;

                                                        case Providers.OleDb_DB2 or Providers.OleDb:
                                                            xparameter = _OleDbCommand.Parameters.AddWithValue(ParameterName, value);
                                                            SetParameter(ref xparameter, Filter.DbColumn);
                                                            break;

                                                        case Providers.SqlServer:
                                                            xparameter = _SqlCommand.Parameters.AddWithValue(ParameterName, value);
                                                            break;

                                                        case Providers.DB2_iSeries :
                                                            xparameter = _DB2iSeriesCommand .Parameters.AddWithValue(ParameterName, value);
                                                            break;

                                                        case Providers.MySQL:
                                                            xparameter = _MySQLCommand.Parameters.AddWithValue(ParameterName, value);
                                                            break;
                                                        //case Providers.PostgreSQL:
                                                        //    break;

                                                        //case Providers.Sharepoint:
                                                        //    break;

                                                        //case Providers.ConfigDefined:
                                                        //    break;

                                                        //case Providers.Undefined:
                                                        //    break;

                                                        default:

                                                            break;

                                                    }
                                                }
                                                else
                                                {
                                                    //value = Utilities.CallByName(Filter.BoundControl, Filter.PropertyName, Utilities.CallType.Get);
                                                    value = Interaction.CallByName(Filter.BoundControl, Filter.PropertyName, CallType.Get);
                                                    //var xparameter = new object();
                                                    System.Data.Common.DbParameter xparameter;
                                                    
                                                    switch (Provider)
                                                    {

                                                        case Providers.OracleDataAccess:
#if COMPILEORACLE
                                                            xparameter = _OracleCommand.Parameters.Add(ParameterName, value);
#endif
                                                            break;

                                                        case Providers.ODBC_DB2 or Providers.ODBC:
                                                            xparameter = _OdbcCommand.Parameters.AddWithValue(ParameterName, value);
                                                            SetParameter(ref xparameter, Filter.DbColumn);
                                                            break;

                                                        case Providers.OleDb_DB2 or Providers.OleDb:
                                                            xparameter = _OleDbCommand.Parameters.AddWithValue(ParameterName, value);
                                                            SetParameter(ref xparameter, Filter.DbColumn);
                                                            break;

                                                        case Providers.SqlServer:
                                                            xparameter = _SqlCommand.Parameters.AddWithValue(ParameterName, value);
                                                            break;

                                                        case Providers.DB2_iSeries :
                                                            xparameter = _DB2iSeriesCommand .Parameters.AddWithValue(ParameterName, value);
                                                            break;

                                                        case Providers.MySQL:
                                                            xparameter = _MySQLCommand.Parameters.AddWithValue(ParameterName, value);
                                                            break;

                                                        default:
                                                            break;
                                                    }
                                                }
                                            }
                                            _i = _i + 1;
                                            switch (ParametersStyle)
                                            {
                                                case ParameterStyles.Qualified:
                                                    {
                                                        _IN.Append(ParameterName + ",");
                                                        break;
                                                    }

                                                default:
                                                    {
                                                        _IN.Append("?,");
                                                        break;
                                                    }
                                            }
                                        }

                                        string _sIN = _IN.ToString();
                                        _sIN = _sIN.Substring(0, _sIN.Length - 1);
                                        s_SQL.Append(Filter.DbColumn.DbColumnName + sComparisionOperators[(int)Filter.ComparisionOperator] + "(" + _sIN + ") " + sLogicsOperators[(int)Filter.LogicOperator] + " ");
                                        break;
                                    }

                                default:
                                    {
                                        //object xparameter;
                                        System.Data.Common.DbParameter xparameter;
                                        
                                        ParameterName = Filter.DbColumn.DbObject.DbConfig.ParameterNamePrefixE + (i.ToString().Trim());
                                        if (DbCommand == null == false)
                                        {
                                            if (Filter.UseBoundControl == false)
                                            {
                                                switch (Provider)
                                                {
                                                    case Providers.OracleDataAccess:
#if COMPILEORACLE
                                                        xparameter = _OracleCommand.Parameters.Add(ParameterName, Filter.Value);
#endif
                                                        break;
                                                    case Providers.ODBC_DB2 or Providers.ODBC:
                                                        xparameter = _OdbcCommand.Parameters.AddWithValue(ParameterName, Filter.Value);
                                                        SetParameter(ref xparameter, Filter.DbColumn);
                                                        break;
                                                    case Providers.OleDb_DB2 or Providers.OleDb:
                                                        xparameter = _OleDbCommand.Parameters.AddWithValue(ParameterName, Filter.Value);
                                                        //xparameter = _OleDbCommand.Parameters.Add(ParameterName, Filter.Value);
                                                        SetParameter(ref xparameter, Filter.DbColumn);
                                                        break;
                                                    case Providers.SqlServer:
                                                        xparameter = _SqlCommand.Parameters.AddWithValue(ParameterName, Filter.Value);
                                                        break;
                                                    case Providers.DB2_iSeries :
                                                        xparameter = _DB2iSeriesCommand.Parameters.AddWithValue(ParameterName, Filter.Value);
                                                        break;

                                                    case Providers.MySQL:
                                                        xparameter = _MySQLCommand.Parameters.AddWithValue(ParameterName, Filter.Value);
                                                        break;
                                                    default:
                                                        break;
                                                }
                                            }
                                            else
                                            {
                                                Filter.Value = Interaction.CallByName(Filter.BoundControl, Filter.PropertyName, CallType.Get);
                                                switch (Filter.DbColumn.DbObject.DbConfig.Provider)
                                                {
                                                    case Providers.OracleDataAccess:
#if COMPILEORACLE
                                                        xparameter = _OracleCommand.Parameters.Add(ParameterName, Filter.Value);
#endif
                                                        break;

                                                    case Providers.ODBC_DB2 or Providers.ODBC:
                                                        xparameter = _OdbcCommand.Parameters.AddWithValue(ParameterName, Filter.Value);
                                                        SetParameter(ref xparameter, Filter.DbColumn);
                                                        break;
                                                    case Providers.OleDb_DB2 or Providers.OleDb:
                                                        xparameter = _OleDbCommand.Parameters.AddWithValue(ParameterName, Filter.Value);
                                                        SetParameter(ref xparameter, Filter.DbColumn);
                                                        break;
                                                    case Providers.SqlServer:
                                                        xparameter = _SqlCommand.Parameters.AddWithValue(ParameterName, Filter.Value);
                                                        break;
                                                    case Providers.DB2_iSeries:
                                                        xparameter = _DB2iSeriesCommand.Parameters.AddWithValue(ParameterName, Filter.Value);
                                                        break;
                                                    case Providers.MySQL:
                                                        xparameter = _MySQLCommand.Parameters.AddWithValue(ParameterName, Filter.Value);
                                                        break;
                                                    default:
                                                        break;
                                                }
                                            }
                                        }

                                        switch (ParametersStyle)
                                        {
                                            case ParameterStyles.Qualified:
                                                {
                                                    s_SQL.Append(Filter.DbColumn.DbColumnName + sComparisionOperators[(int)Filter.ComparisionOperator] + ParameterName + " " + sLogicsOperators[(int)Filter.LogicOperator] + " ");
                                                    break;
                                                }

                                            default:
                                                {
                                                    s_SQL.Append(Filter.DbColumn.DbColumnName + sComparisionOperators[(int)Filter.ComparisionOperator] + " ? " + sLogicsOperators[(int)Filter.LogicOperator] + " ");
                                                    break;
                                                }
                                        }

                                        break;
                                    }
                            }

                            i = i + 1;
                        }

                        Index1 = Index1 + 1;
                    }
                    s_SQL.Append(") " + sLogicsOperators[(int)Filters.LogicOperator] + " ");
                    Index0 = Index0 + 1;
                    CachedSQLWhereStmt = s_SQL.ToString();
                }



                if (_Cache == true)
                {
                    // CACHE 
                    Index1 = 0;
                    foreach (DbFilter currentFilter1 in Filters)
                    {
                        Filter = currentFilter1;
                        if (Index1 == Filters.Count - 1)
                            Filter.LogicOperator = LogicOperator.None;
                        value = null;
                        if (!string.IsNullOrEmpty(Filter.DbColumn.DbColumnName))
                        {
                            switch (Filter.ComparisionOperator)
                            {
                                case ComparisionOperator.Between or ComparisionOperator.NotBetween:
                                    {

                                        object _b1 = null;
                                        object _b2 = null;
                                        if (Filter.Value.GetType().IsArray == true)
                                        {
                                            dynamic _Filter = Filter;
                                            _b1 = _Filter.Value(0);
                                            _b2 = _Filter.Value(1);

                                        }

                                        if (Filter.Value.GetType().IsGenericType == true)
                                        {
                                            dynamic _Filter = Filter;
                                            _b1 = _Filter.Value(0);
                                            _b2 = _Filter.Value(1);

                                        }

                                        string Parameter1;
                                        string Parameter2;
                                        string xi = i.ToString().Trim();
                                        Parameter1 = Filter.DbColumn.DbObject.DbConfig.ParameterNamePrefixE + xi + "_1";
                                        Parameter2 = Filter.DbColumn.DbObject.DbConfig.ParameterNamePrefixE + xi + "_2";
                                        if (DbCommand == null == false)
                                        {
                                            if (Filter.UseBoundControl == false)
                                            {
                                                DbCommand.Parameters[Parameter1].Value = _b1;
                                                DbCommand.Parameters[Parameter2].Value = _b2;
                                            }
                                            else
                                            {
                                                value = Interaction.CallByName(Filter.BoundControl, Filter.PropertyName, CallType.Get);
                                                DbCommand.Parameters[Parameter1].Value = value;
                                                value = Interaction.CallByName(Filter.BoundControl, Filter.PropertyName, CallType.Get);
                                                DbCommand.Parameters[Parameter2].Value = value;
                                            }
                                        }

                                        break;
                                    }

                                case ComparisionOperator.In or ComparisionOperator.NotIn:
                                    {
                                        int _i = 1;
                                        string xi = "";
                                        foreach (var currentValue1 in (IEnumerable)Filter.Value)
                                        {
                                            value = currentValue1;
                                            xi = _i.ToString().Trim();
                                            ParameterName = Filter.DbColumn.DbObject.DbConfig.ParameterNamePrefixE + i + "_" + xi;
                                            if (DbCommand != null)
                                            {
                                                if (Filter.UseBoundControl == false)
                                                {
                                                    DbCommand.Parameters[ParameterName].Value = value;
                                                }
                                                else
                                                {
                                                    value = Interaction.CallByName(Filter.BoundControl, Filter.PropertyName, CallType.Get);
                                                    DbCommand.Parameters[ParameterName].Value = value;
                                                }
                                            }
                                            _i++;
                                        }

                                        break;
                                    }

                                default:
                                    {
                                        ParameterName = Filter.DbColumn.DbObject.DbConfig.ParameterNamePrefixE + i.ToString().Trim();
                                        if (DbCommand == null == false)
                                        {
                                            if (Filter.UseBoundControl == false)
                                            {


                                                switch (Filter.DbColumn.DbObject.DbConfig.Provider)
                                                {

                                                    default:
                                                        {
                                                            DbCommand.Parameters[ParameterName].Value = Filter.Value;
                                                            break;
                                                        }
                                                }
                                            }
                                            else
                                            {
                                                //value = Utilities.CallByName(Filter.BoundControl, Filter.PropertyName, Utilities.CallType.Get);
                                                value = Interaction.CallByName(Filter.BoundControl, Filter.PropertyName, CallType.Get);
                                                switch (Filter.DbColumn.DbObject.DbConfig.Provider)
                                                {
                                                    default:
                                                        {
                                                            DbCommand.Parameters[ParameterName].Value = Filter.Value;
                                                            break;
                                                        }
                                                }
                                            }
                                        }

                                        break;
                                    }
                            }
                            i = i + 1;
                        }
                        Index1 = Index1 + 1;
                    }
                    Index0 = Index0 + 1;
                }
            }


            _SQL = s_SQL.ToString();
            if (_SQL.Trim() == "()")
                _SQL = "";
            if (!string.IsNullOrEmpty(_SQL))
                SQL = _SQL;

            if (_Cache == true & Cache == true)
            {
                SQL = CachedSQLWhereStmt;
            }

            BuildSQLFilterRet = true;
            return BuildSQLFilterRet;
        }



        

        //public void SetParameter(ref dynamic  Parameter, DbColumn DbColumn)
        public void SetParameter(ref System.Data.Common .DbParameter  Parameter, DbColumn DbColumn)
        {
            Parameter.DbType = DbColumn.DbType;
            Parameter.Size = DbColumn.Size;
            //Parameter.Scale = (byte)DbColumn.NumericScale;
          
        }
    }
    #endregion


    public class DbLookUp
    {
        private DbObject mDbObject;
        private DbColumns mBoundControls = new DbColumns();
        private DbColumn mLookUpDbColumn;
        private object mLookUpValue;
        private bool mValidated = false;
        private DbColumn mForeignKeyDbColumn;
        private DbFilters mFilters = new DbFilters();
        private bool mEnabled = true;
        private ConnecteDbLookUps mConnectedDbLookUps = new ConnecteDbLookUps();

        public event DbObject_ChangedEventHandler DbObject_Changed;

        public delegate void DbObject_ChangedEventHandler();

        public DbLookUp()
        {
        }

        public ConnecteDbLookUps ConnectedDbLookUps
        {
            get
            {
                ConnecteDbLookUps ConnectedDbLookUpsRet = default;
                ConnectedDbLookUpsRet = mConnectedDbLookUps;
                return ConnectedDbLookUpsRet;
            }

            set
            {
                mConnectedDbLookUps = value;
            }
        }

        public bool Enabled
        {
            get
            {
                bool EnabledRet = default;
                EnabledRet = mEnabled;
                return EnabledRet;
            }

            set
            {
                mEnabled = value;
            }
        }

        public DbColumn ForeignKeyDbColumn
        {
            get
            {
                DbColumn ForeignKeyDbColumnRet = default;
                ForeignKeyDbColumnRet = mForeignKeyDbColumn;
                return ForeignKeyDbColumnRet;
            }

            set
            {
                mForeignKeyDbColumn = value;
            }
        }

        public DbFilters Filters
        {
            get
            {
                DbFilters FiltersRet = default;
                FiltersRet = mFilters;
                return FiltersRet;
            }

            set
            {
                mFilters = value;
            }
        }

        public bool Validated
        {
            get
            {
                bool ValidatedRet = default;
                ValidatedRet = mValidated;
                return ValidatedRet;
            }
        }

        public object LookUpValue
        {
            get
            {
                object LookUpValueRet = default;
                LookUpValueRet = mLookUpValue;
                return LookUpValueRet;
            }

            set
            {
                mLookUpValue = value;
            }
        }

        public DbColumn LookupDbColumn
        {
            get
            {
                DbColumn LookupDbColumnRet = default;
                LookupDbColumnRet = mLookUpDbColumn;
                return LookupDbColumnRet;
            }

            set
            {
                mLookUpDbColumn = value;
            }
        }

        public DbColumns BoundControls
        {
            get
            {
                DbColumns BoundControlsRet = default;
                BoundControlsRet = mBoundControls;
                return BoundControlsRet;
            }

            set
            {
                mBoundControls = value;
            }
        }

        public DbObject DbObject
        {
            get
            {
                DbObject DbObjectRet = default;
                DbObjectRet = mDbObject;
                return DbObjectRet;
            }

            set
            {
                mDbObject = value;
                DbObject_Changed?.Invoke();
            }
        }

        public void LookUpByFilters()
        {
            pLookUpByFilters(true);
        }

        public bool ValidateByFilters()
        {
            return pLookUpByFilters(false);
        }

        private bool pLookUpByFilters(bool Mode)
        {
            if (mEnabled == false)
                return default;
            if (mDbObject is null)
            {
                return false;
            }

            DataSet ds = null;
            DataRowCollection dr = null;
            //DbFilter filter = null;
            DbLookUp DbLookUp = null;
            int rcount = 0;
            int cDataBinding = 0;
            mValidated = false;
            if (mFilters is null)
            {
                return false;
            }

            mDbObject.FiltersGroup.Clear();
            mDbObject.FiltersGroup.Add(mFilters);
            cDataBinding = (int)mDbObject.DataBinding;
            mDbObject.DataBinding = DataBoundControlsBehaviour.NoDataBinding;
            mDbObject.SuppressErrorsNotification = true;
            ds = mDbObject.ExecuteDataSet();
            mDbObject.SuppressErrorsNotification = false;

            // If ds.Tables.Count = 0 Then Return False

            dr = ds.Tables[0].Rows;
            rcount = dr.Count;
            if (rcount > 0)
            {
                mValidated = true;
            }

            mDbObject.DataBinding = (DataBoundControlsBehaviour)cDataBinding;
            if (Convert.ToInt32(Mode) == 0)
            {
                return mValidated;
            }

            object Value = null;
            foreach (DbLookUp currentDbLookUp in mConnectedDbLookUps)
            {
                DbLookUp = currentDbLookUp;
                DbLookUp.Enabled = false;
            }

            foreach (DbColumn dbcolumn in mBoundControls)
            {
                if (rcount < 1)
                {
                    Value = null;
                }
                else
                {
                    Value = dr[0][dbcolumn.DbColumnNameE];
                }

                if (ReferenceEquals(Value, DBNull.Value))
                {
                    Value = null;
                }

                foreach (BoundControl BoundControl in dbcolumn.BoundControls)
                {
                    try
                    {
                        
                        //Utilities.CallByName(BoundControl.Control, BoundControl.PropertyName, Utilities.CallType.Set, Value);
                        Interaction.CallByName(BoundControl.Control, BoundControl.PropertyName, CallType.Set, Value);
                    }
                    catch
                    {
                    }
                }
            }

            mDbObject.FiltersGroup.Clear();
            dr = null;
            ds.Dispose();
            foreach (DbLookUp currentDbLookUp1 in mConnectedDbLookUps)
            {
                DbLookUp = currentDbLookUp1;
                DbLookUp.Enabled = true;
            }

            return mValidated;
        }

        public void LookUp(object LookUpValue)
        {
            pLookUp(LookUpValue, true);
        }

        public bool Validate(object LookupValue)
        {
            return pLookUp(LookupValue, false);
        }

        private bool pLookUp(object LookUpValue, bool Mode)
        {
            if (mEnabled == false)
                return default;
            if (mDbObject is null)
            {
                return false;
            }

            DataSet ds = null;
            DataRowCollection dr = null;
            var F1 = new DbFilters();
            int rcount = 0;
            DbLookUp DbLookUp = null;
            mValidated = false;
            mLookUpValue = LookUpValue;
            F1.Add(mLookUpDbColumn, ComparisionOperator.Equals, LookUpValue, LogicOperator.None);
            mDbObject.FiltersGroup.Clear();
            mDbObject.FiltersGroup.Add(F1);
            mDbObject.DataBinding = DataBoundControlsBehaviour.NoDataBinding;
            ds = mDbObject.ExecuteDataSet();
            dr = ds.Tables[0].Rows;
            rcount = dr.Count;
            if (rcount > 0)
            {
                mValidated = true;
            }

            if (Mode == false)
            {
                return mValidated;
            }

            // If mForeignKeyDbColumn IsNot Nothing Then
            // If mForeignKeyDbColumn.IsForeignKey = True Then
            // mForeignKeyDbColumn.IsForeignKeyValidated = mValidated
            // End If
            // End If

            foreach (DbLookUp currentDbLookUp in mConnectedDbLookUps)
            {
                DbLookUp = currentDbLookUp;
                DbLookUp.Enabled = false;
            }

            object Value;
            foreach (DbColumn dbcolumn in mBoundControls)
            {
                if (rcount < 1)
                {
                    Value = null;
                }
                else
                {
                    Value = dr[0][dbcolumn.DbColumnName];
                }

                if (ReferenceEquals(Value, DBNull.Value))
                {
                    Value = null;
                }

                foreach (BoundControl BoundControl in dbcolumn.BoundControls)
                    //Utilities.CallByName(BoundControl.Control, BoundControl.PropertyName, Utilities.CallType.Set, Value);
                    Interaction.CallByName(BoundControl.Control, BoundControl.PropertyName, CallType.Set, Value);
            }

            foreach (DbLookUp currentDbLookUp1 in mConnectedDbLookUps)
            {
                DbLookUp = currentDbLookUp1;
                DbLookUp.Enabled = true;
            }

            mDbObject.FiltersGroup.Clear();
            dr = null;
            ds.Dispose();
            return mValidated;
        }

        public int GetFilterIndexByBoundControl(string ControlName, string PropertyName = "")
        {
            object index = -1;
            DbFilter _DbFilter;
            string Name = "";
            PropertyName = PropertyName.Trim().ToLower();
            //for (int i = 0, loopTo = mFilters.Count - 1; i <= loopTo; i++)
            for (int i = 0; i < mFilters.Count; i++)
            {
                _DbFilter = (DbFilter)mFilters.get_Item(i);
                if (_DbFilter.BoundControl is object)
                {
                    //Name = Convert.ToString(Utilities.CallByName(_DbFilter.BoundControl, "Name", Utilities.CallType.Get));
                    Name = Convert.ToString(Interaction.CallByName(_DbFilter.BoundControl, "Name", CallType.Get));
                    bool exitFor = false;
                    bool exitFor1 = false;
                    switch (PropertyName ?? "")
                    {
                        case "":
                            {
                                if ((Name ?? "") == (ControlName ?? ""))
                                {
                                    index = i;
                                    exitFor = true;
                                    break;
                                }

                                break;
                            }

                        default:
                            {
                                if ((Name ?? "") == (ControlName ?? "") & (_DbFilter.PropertyName.Trim().ToLower() ?? "") == (PropertyName ?? ""))
                                {
                                    index = i;
                                    exitFor1 = true;
                                    break;
                                }

                                break;
                            }
                    }

                    if (exitFor)
                    {
                        break;
                    }

                    if (exitFor1)
                    {
                        break;
                    }
                }
            }

            return Convert.ToInt32(index);
        }

        public DbColumn GetDbColumnByBoundControl(string DbColumnNameE, string ControlName, string PropertyName = "")
        {
            ControlName = ControlName.Trim();
            PropertyName = PropertyName.Trim().ToLower();
            string Name = "";
            foreach (DbColumn _DbColumn in mBoundControls)
            {
                if ((_DbColumn.DbColumnNameE ?? "") == (DbColumnNameE.Trim() ?? ""))
                {
                    if (_DbColumn.BoundControls is object)
                    {
                        foreach (object _BoundControl in _DbColumn.BoundControls)
                        {
                            //Name = Convert.ToString(Utilities.CallByName(_BoundControl, "Name", Utilities.CallType.Get));
                            Name = Convert.ToString(Interaction.CallByName(_BoundControl, "Name", CallType.Get));
                            switch (PropertyName ?? "")
                            {
                                case "":
                                    {
                                        if ((Name) == (ControlName))
                                        {
                                            return _DbColumn;
                                        }

                                        break;
                                    }

                                default:
                                    {
                                        if (Name == ControlName & _BoundControl != null)
                                        {
                                            return _DbColumn;
                                        }
                                        break;
                                    }
                            }

                            if ((Name ?? "") == (ControlName.Trim() ?? ""))
                            {
                                return _DbColumn;
                            }
                        }
                    }
                }
            }

            return null;
        }

        public int GetDbColumnIndexByBoundControl(string DbColumnNameE, string ControlName)
        {
            ControlName = ControlName.Trim();
            DbColumn _DbColumn;
            string Name;
            for (int i = 0, loopTo = mBoundControls.Count - 1; i <= loopTo; i++)
            {
                _DbColumn = (DbColumn)mBoundControls.get_Item(i);
                if ((_DbColumn.DbColumnNameE ?? "") == (DbColumnNameE.Trim() ?? ""))
                {
                    if (_DbColumn.BoundControls is object)
                    {
                        foreach (object _BoundControl in _DbColumn.BoundControls)
                        {
                            //Name = Convert.ToString(Utilities.CallByName(_BoundControl, "Name", Utilities.CallType.Get));
                            Name = Convert.ToString(Interaction.CallByName(_BoundControl, "Name", CallType.Get));
                            if ((Name ?? "") == (ControlName.Trim() ?? ""))
                            {
                                return i;
                            }
                        }
                    }
                }
            }

            return -1;
        }

        public DbFilter GetFilterByBoundControl(string ControlName, string PropertyName = "")
        {
            ControlName = ControlName.Trim();
            PropertyName = PropertyName.Trim().ToLower();
            DbFilter DbFilter = null;
            string Name = "";
            foreach (DbFilter _DbFilter in mFilters)
            {
                if (_DbFilter.BoundControl is object)
                {
                    Name = Convert.ToString(Interaction.CallByName(_DbFilter.BoundControl, "Name", CallType.Get));
                    bool exitFor = false;
                    bool exitFor1 = false;
                    switch (PropertyName ?? "")
                    {
                        case "":
                            {
                                if ((Name ?? "") == (ControlName ?? ""))
                                {
                                    DbFilter = _DbFilter;
                                    exitFor = true;
                                    break;
                                }

                                break;
                            }

                        default:
                            {
                                if ((Name ?? "") == (ControlName ?? "") & (_DbFilter.PropertyName.Trim().ToLower() ?? "") == (PropertyName ?? ""))
                                {
                                    DbFilter = _DbFilter;
                                    exitFor1 = true;
                                    break;
                                }

                                break;
                            }
                    }

                    if (exitFor)
                    {
                        break;
                    }

                    if (exitFor1)
                    {
                        break;
                    }
                }
            }

            return DbFilter;
        }
    }

    public class DbRelationship
    {
        private string mDescription;
        private DbRelationshipItems mDbRelationshipItems = new DbRelationshipItems();
        private DbObject mDbObject;
        private DbObject mDbObjectLeft;
        private DbObject mDbObjectRight;

        public DbObject DbObjectLeft
        {
            get
            {
                DbObject DbObjectLeftRet = default;
                DbObjectLeftRet = mDbObjectLeft;
                return DbObjectLeftRet;
            }

            set
            {
                mDbObjectLeft = value;
            }
        }

        public DbObject DbObjectRight
        {
            get
            {
                DbObject DbObjectRightRet = default;
                DbObjectRightRet = mDbObjectRight;
                return DbObjectRightRet;
            }

            set
            {
                mDbObjectRight = value;
            }
        }

        public DbObject DbObject
        {
            get
            {
                DbObject DbObjectRet = default;
                DbObjectRet = mDbObject;
                return DbObjectRet;
            }

            set
            {
                mDbObject = value;
            }
        }

        public DbRelationshipItems RelationShipItems
        {
            get
            {
                DbRelationshipItems RelationShipItemsRet = default;
                RelationShipItemsRet = mDbRelationshipItems;
                return RelationShipItemsRet;
            }

            set
            {
                mDbRelationshipItems = value;
            }
        }

        public string Description
        {
            get
            {
                string DescriptionRet = default;
                DescriptionRet = mDescription;
                return DescriptionRet;
            }

            set
            {
                mDescription = value;
            }
        }

        public void Do()
        {
            var F1 = new DbFilters();
            int i = 0;
            int c = 0;
            LogicOperator cOperator;
            c = RelationShipItems.Count - 1;
            foreach (DbRelationshipItem Item in RelationShipItems)
            {
                if (i < c)
                {
                    cOperator = LogicOperator.AND;
                }
                else
                {
                    cOperator = LogicOperator.None;
                }

                F1.Add(Item.DbColumnNameRight, ComparisionOperator.Equals, Item.DbColumnNameLeft.Value, cOperator);
                // Threading.Thread.Sleep(0)
            }

            F1.get_Item(c).LogicOperator = LogicOperator.None;
            {
                ref var withBlock = ref mDbObject;
                withBlock.FiltersGroup.Clear();
                withBlock.FiltersGroup.Add(F1);
                withBlock.BlankDbColumns(withBlock.DataBinding);
                withBlock.DoQuery();
                withBlock.MoveFirst();
            }
        }

        public void DoRelationShipLeft(bool BindingBehaviour)
        {
            var F1 = new DbFilters();
            int i = 0;
            int c = 0;
            LogicOperator cOperator;
            c = RelationShipItems.Count - 1;
            foreach (DbRelationshipItem Item in RelationShipItems)
            {
                if (i < c)
                {
                    cOperator = LogicOperator.AND;
                }
                else
                {
                    cOperator = LogicOperator.None;
                }
                // If Item.DbColumnNameLeft.Value Is DBNull.Value Then Return
                F1.Add(Item.DbColumnNameRight, ComparisionOperator.Equals, Item.DbColumnNameLeft.Value, cOperator);
                // Threading.Thread.Sleep(0)
            }

            mDbObjectRight.BindingBehaviour = BindingBehaviour;
            F1.get_Item(c).LogicOperator = LogicOperator.None;
            {
                ref var withBlock = ref mDbObjectRight;
                withBlock.FiltersGroup.Clear();
                withBlock.FiltersGroup.Add(F1);
                withBlock.BlankDbColumns(withBlock.DataBinding);
                withBlock.DoQuery();
                withBlock.MoveFirst();
            }
        }

        public void DoRelationShipRight()
        {
            var F1 = new DbFilters();
            int i = 0;
            int c = 0;
            LogicOperator cOperator;
            c = RelationShipItems.Count - 1;
            foreach (DbRelationshipItem Item in RelationShipItems)
            {
                if (i < c)
                {
                    cOperator = LogicOperator.AND;
                }
                else
                {
                    cOperator = LogicOperator.None;
                }

                F1.Add(Item.DbColumnNameLeft, ComparisionOperator.Equals, Item.DbColumnNameRight.Value, cOperator);
                // Threading.Thread.Sleep(0)
            }

            F1.get_Item(c).LogicOperator = LogicOperator.None;
            {
                ref var withBlock = ref mDbObjectLeft;
                withBlock.FiltersGroup.Clear();
                withBlock.FiltersGroup.Add(F1);
                withBlock.BlankDbColumns(withBlock.DataBinding);
                withBlock.DoQuery();
                withBlock.MoveFirst();
            }
        }
    }

    public class DbRelationshipItem
    {
        private DbColumn mDbColumnNameLeft;
        private DbColumn mDbColumnNameRight;

        public DbColumn DbColumnNameLeft
        {
            get
            {
                DbColumn DbColumnNameLeftRet = default;
                DbColumnNameLeftRet = mDbColumnNameLeft;
                return DbColumnNameLeftRet;
            }

            set
            {
                mDbColumnNameLeft = value;
            }
        }

        public DbColumn DbColumnNameRight
        {
            get
            {
                DbColumn DbColumnNameRightRet = default;
                DbColumnNameRightRet = mDbColumnNameRight;
                return DbColumnNameRightRet;
            }

            set
            {
                mDbColumnNameRight = value;
            }
        }
    }

    public class DbRelationshipItems : CollectionBase
    {
        public bool Add(DbColumn DbColumnNameLeft, DbColumn DbColumnNameRight)
        {
            try
            {
                var x = new DbRelationshipItem();
                x.DbColumnNameLeft = DbColumnNameLeft;
                x.DbColumnNameRight = DbColumnNameRight;
                List.Add(x);
                return true;
            }
            catch
            {
                return false;
            }
        }

        public bool Add(DbRelationshipItem DbRelationShipItem)
        {
            try
            {
                List.Add(DbRelationShipItem);
                return true;
            }
            catch
            {
                return false;
            }
        }

        public DbRelationshipItem get_Item(int Index)
        {
            DbRelationshipItem ItemRet = default;
            ItemRet = (DbRelationshipItem)List[Index];
            return ItemRet;
        }

        public void set_Item(int Index, DbRelationshipItem value)
        {
            List[Index] = value;
        }

        public DbRelationshipItems()
        {
        }
    }

    public class DbJoin
    {
        private JoinType mJoinType;
        private string mName;
        private DbJoinConditions mJoinConditions = new DbJoinConditions();
        private string[] sLogicsOperators = new string[] { "", "AND", "OR", "NOT" };
        private string[] sComparisionOperators = new string[] { " = ", " > ", " < ", " >= ", " <= ", " <> ", " BETWEEN", " IN ", " LIKE ", " NOT LIKE ", " EXISTS ", "" };

        public JoinType JoinType
        {
            get
            {
                JoinType JoinTypeRet = default;
                JoinTypeRet = mJoinType;
                return JoinTypeRet;
            }

            set
            {
                mJoinType = value;
            }
        }

        public DbJoinConditions JoinConditions
        {
            get
            {
                DbJoinConditions JoinConditionsRet = default;
                JoinConditionsRet = mJoinConditions;
                return JoinConditionsRet;
            }

            set
            {
                mJoinConditions = value;
            }
        }

        public string SQLJoin()
        {
            string SQLJoinRet = default;
            string SQL = "";
            if (Convert.ToBoolean(mJoinConditions.Count))
            {
                switch (JoinType)
                {
                    case JoinType.FullOuterJoin:
                        {
                            SQL = " FULL OUTER JOIN ";
                            break;
                        }

                    case JoinType.InnerJoin:
                        {
                            SQL = " INNER JOIN ";
                            break;
                        }

                    case JoinType.LeftOuterJoin:
                        {
                            SQL = " LEFT OUTER JOIN ";
                            break;
                        }

                    case JoinType.RightOuterJoin:
                        {
                            SQL = " RIGHT OUTER JOIN ";
                            break;
                        }

                    default:
                        {
                            SQL = " INNER JOIN ";
                            break;
                        }
                }
            }

            int i = 0;
            int u = mJoinConditions.Count;
            foreach (DbJoinCondition JoinCondition in mJoinConditions)
            {
                if (i == 0)
                {
                    SQL = SQL + JoinCondition.DbColumnRight.TableName + " ON (";
                }

                SQL = SQL + JoinCondition.DbColumnLeft.DbColumnName;
                SQL = SQL + sComparisionOperators[(int)JoinCondition.ComparisionOperator];
                SQL = SQL + JoinCondition.DbColumnRight.DbColumnName + " ";
                if (i < u)
                {
                    SQL = SQL + sLogicsOperators[(int)JoinCondition.LogicOperator] + " ";
                }

                i = i + 1;
                // Threading.Thread.Sleep(0)
            }

            SQL = SQL + ")";
            SQLJoinRet = SQL;
            return SQLJoinRet;
        }
    }

    public class DbJoinConditions : CollectionBase
    {
        public bool Add(DbColumn DbColumnLeft, DbColumn DbColumnRight, ComparisionOperator ComparisionOperator, LogicOperator LogicOperator)
        {
            try
            {
                var x = new DbJoinCondition();
                x.DbColumnLeft = DbColumnLeft;
                x.DbColumnRight = DbColumnRight;
                x.ComparisionOperator = ComparisionOperator;
                x.LogicOperator = LogicOperator;
                List.Add(x);
                return true;
            }
            catch
            {
                return false;
            }
        }

        public DbJoinCondition get_Item(int Index)
        {
            DbJoinCondition ItemRet = default;
            ItemRet = (DbJoinCondition)List[Index];
            return ItemRet;
        }

        public void set_Item(int Index, DbJoinCondition value)
        {
            List[Index] = value;
        }
    }

    public class DbJoinCondition
    {
        private DbColumn mDbColumnLeft;
        private DbColumn mDbColumnRight;
        private ComparisionOperator mComparisionOperator;
        private LogicOperator mLogicOperator;
        private object mValue;

        public DbJoinCondition()
        {
        }

        public DbJoinCondition(DbColumn DbColumnLeft, DbColumn DbColumnRight, ComparisionOperator ComparisionOperator, object Value)
        {
            mDbColumnLeft = DbColumnLeft;
            mDbColumnRight = DbColumnRight;
            mLogicOperator = LogicOperator;
            mComparisionOperator = ComparisionOperator;
            mValue = Value;
        }

        public object Value
        {
            get
            {
                object ValueRet = default;
                ValueRet = mValue;
                return ValueRet;
            }

            set
            {
                mValue = value;
            }
        }

        public LogicOperator LogicOperator
        {
            get
            {
                LogicOperator LogicOperatorRet = default;
                LogicOperatorRet = mLogicOperator;
                return LogicOperatorRet;
            }

            set
            {
                mLogicOperator = value;
            }
        }

        public ComparisionOperator ComparisionOperator
        {
            get
            {
                ComparisionOperator ComparisionOperatorRet = default;
                ComparisionOperatorRet = mComparisionOperator;
                return ComparisionOperatorRet;
            }

            set
            {
                mComparisionOperator = value;
            }
        }

        public DbColumn DbColumnRight
        {
            get
            {
                DbColumn DbColumnRightRet = default;
                DbColumnRightRet = mDbColumnRight;
                return DbColumnRightRet;
            }

            set
            {
                mDbColumnRight = value;
            }
        }

        public DbColumn DbColumnLeft
        {
            get
            {
                DbColumn DbColumnLeftRet = default;
                DbColumnLeftRet = mDbColumnLeft;
                return DbColumnLeftRet;
            }

            set
            {
                mDbColumnLeft = value;
            }
        }
    }

    public class DbObjects : CollectionBase
    {
        public DbObject get_Item(int Index)
        {
            DbObject ItemRet = default;
            ItemRet = (DbObject)List[Index];
            return ItemRet;
        }

        public void set_Item(int Index, DbObject value)
        {
            List[Index] = value;
        }

        public void Add(DbObject DbObject)
        {
            List.Add(DbObject);
        }
    }

    public class ConnecteDbLookUps : CollectionBase
    {
        public DbLookUp get_Item(int Index)
        {
            DbLookUp ItemRet = default;
            ItemRet = (DbLookUp)List[Index];
            return ItemRet;
        }

        public void set_Item(int Index, DbLookUp value)
        {
            List[Index] = value;
        }

        public void Add(DbLookUp DbLookUp)
        {
            List.Add(DbLookUp);
        }
    }

    public class DbObjectAdapter<T>
    {
        public ExecutionResult FromDbObjectToList(DbObject DBObject, ref List<T> List)
        {
            return FromDataTableToList(DBObject.DataTable, ref List);
        }

        public ExecutionResult ToList(DbObject DBObject, ref List<T> List)
        {
            return FromDataTableToList(DBObject.DataTable, ref List);
        }

        public ExecutionResult FromDataTableToList(DataTable dataTable, ref List<T> List)  // List(Of T)
        {
            // This create a new list with the same type of the generic class
            var ER = new ExecutionResult();
            ER.Context = "DbObjectAdapter:FromDataTableToList";
            System.Collections.Generic.List<T> _List = new List<T>();
            // Obtains the type of the generic class
            var _TypeT = typeof(T);
            if (dataTable is null == true)
            {
                return null;
            }

            // Obtains the properties definition of the generic class.
            // With this, we are going to know the property names of the class
            var pi = _TypeT.GetProperties();
            var fi = _TypeT.GetFields();
            object columnvalue;
            // For each row in the datatable

            foreach (DataRow row in dataTable.Rows)
            {
                // Create a new instance of the generic class
                var defaultInstance = Activator.CreateInstance(_TypeT);
                // For each property in the properties of the class
                foreach (PropertyInfo prop in pi)
                {
                    try
                    {
                        // Get the value of the row according to the field name
                        // Remember that the classïs members and the tableïs field names
                        // must be identical
                        columnvalue = row[prop.Name];
                        // Know check if the value is null. 
                        // If not, it will be added to the instance
                        if (!ReferenceEquals(columnvalue, DBNull.Value))
                        {
                            prop.SetValue(defaultInstance, columnvalue, null);
                        }
                    }
                    catch
                    {
                        // Console.WriteLine(prop.Name & ": " & ex.ToString())
                        // Return Nothing
                    }
                    // Threading.Thread.Sleep(0)
                }

                foreach (FieldInfo prop in fi)
                {
                    try
                    {
                        // Get the value of the row according to the field name
                        // Remember that the classïs members and the tableïs field names
                        // must be identical
                        columnvalue = row[prop.Name];
                        // Know check if the value is null. 
                        // If not, it will be added to the instance
                        // created with Activator.CreateInstance(t)

                        if (!ReferenceEquals(columnvalue, DBNull.Value))
                        {
                            // Set the value dinamically. Now you need to pass as an argument
                            // an instance class of the generic class. This instance has been
                            // created with Activator.CreateInstance(t)
                            if (prop.IsPrivate)
                            {
                                prop.SetValue(defaultInstance, columnvalue);
                            }

                            if (prop.IsPublic == true)
                            {
                                //Utilities.CallByName(defaultInstance, prop.Name, Utilities.CallType.Set, columnvalue);
                                Interaction.CallByName(defaultInstance, prop.Name, CallType.Set, columnvalue);
                            }
                        }
                        else
                        {
                            // If prop.FieldType.Name = "DateTime" Then
                            // If prop.IsPrivate Then
                            // prop.SetValue(defaultInstance, New Nullable(Of Date))
                            // End If
                            // If prop.IsPublic = True Then
                            // CallByName(defaultInstance, prop.Name, CallType.Set, New Nullable(Of Date))
                            // End If
                            // End If
                        }
                    }
                    catch (Exception ex)
                    {
                        ER.ResultCode = ExecutionResult.eResultCode.Failed;
                        ER.ResultMessage = ex.Message;
                        ER.ErrorCode = -1; //Information.Err().Number;
                        ER.Exception = ex;
                        // Console.WriteLine(prop.Name & ": " & ex.ToString())
                        return ER;
                    }
                    // Threading.Thread.Sleep(0)
                }
                // Now, create a class of the same type of the generic class. 
                // Then a conversion is done to set the value
                T myclass = (T)defaultInstance;
                // Add the generic instance to the generic list
                _List.Add(myclass);
                // Threading.Thread.Sleep(0)
            }
            // At this moment, the generic list contains all the datatable values
            if (ER.Success)
            {
                List = (List<T>)_List;
            }

            return ER;
        }

        public ExecutionResult FromDataRowToObject(DataRow DataRow, ref T T)  // List(Of T)
        {
            // This create a new list with the same type of the generic class
            var ER = new ExecutionResult();
            ER.Context = "DbObjectAdapter:FromDataTableToList";


            // Obtains the type of the generic class
            var _TypeT = typeof(T);
            if (DataRow is null == true)
            {
                return null;
            }

            // Obtains the properties definition of the generic class.
            // With this, we are going to know the property names of the class
            var pi = _TypeT.GetProperties();
            var fi = _TypeT.GetFields();
            object columnvalue;

            // Create a new instance of the generic class
            var defaultInstance = Activator.CreateInstance(_TypeT);
            // For each property in the properties of the class
            foreach (PropertyInfo prop in pi)
            {
                try
                {
                    // Get the value of the row according to the field name
                    // Remember that the classïs members and the tableïs field names
                    // must be identical
                    columnvalue = DataRow[prop.Name];
                    // Know check if the value is null. 
                    // If not, it will be added to the instance
                    if (!ReferenceEquals(columnvalue, DBNull.Value))
                    {
                        prop.SetValue(defaultInstance, columnvalue, null);
                    }
                }
                catch
                {
                    // Console.WriteLine(prop.Name & ": " & ex.ToString())
                    // Return Nothing
                }
                // Threading.Thread.Sleep(0)
            }

            foreach (FieldInfo prop in fi)
            {
                try
                {
                    // Get the value of the row according to the field name
                    // Remember that the classïs members and the tableïs field names
                    // must be identical
                    columnvalue = DataRow[prop.Name];
                    // Know check if the value is null. 
                    // If not, it will be added to the instance
                    // created with Activator.CreateInstance(t)

                    if (!ReferenceEquals(columnvalue, DBNull.Value))
                    {
                        // Set the value dinamically. Now you need to pass as an argument
                        // an instance class of the generic class. This instance has been
                        // created with Activator.CreateInstance(t)
                        if (prop.IsPrivate)
                        {
                            prop.SetValue(defaultInstance, columnvalue);
                        }

                        if (prop.IsPublic == true)
                        {
                            //Utilities.CallByName(defaultInstance, prop.Name, Utilities.CallType.Set, columnvalue);
                            Interaction.CallByName(defaultInstance, prop.Name, CallType.Set, columnvalue);
                        }
                    }
                    else
                    {
                        // If prop.FieldType.Name = "DateTime" Then
                        // If prop.IsPrivate Then
                        // prop.SetValue(defaultInstance, New Nullable(Of Date))
                        // End If
                        // If prop.IsPublic = True Then
                        // CallByName(defaultInstance, prop.Name, CallType.Set, New Nullable(Of Date))
                        // End If
                        // End If
                    }
                }
                catch (Exception ex)
                {
                    ER.ResultCode = ExecutionResult.eResultCode.Failed;
                    ER.ResultMessage = ex.Message;
                    ER.ErrorCode = -1;// Information.Err().Number;
                    ER.Exception = ex;
                    // Console.WriteLine(prop.Name & ": " & ex.ToString())
                    return ER;
                }
                // Threading.Thread.Sleep(0)
            }
            // Now, create a class of the same type of the generic class. 
            // Then a conversion is done to set the value
            T myclass = (T)defaultInstance;
            // Add the generic instance to the generic list

            // Threading.Thread.Sleep(0)

            // At this moment, the generic list contains all the datatable values
            if (ER.Success)
            {
            }

            return ER;
        }

        public bool FromListToDbObject(List<T> List, ref DbObject DbObject)
        {
            if (DbObject is null == true)
            {
                return false;
            }

            if (DbObject.DataTable is null)
            {
                return false;
            }

            if (List is null == true)
            {
                return false;
            }


            // Obtains the properties definition of the generic class.
            // With this, we are going to know the property names of the class
            var t = typeof(T);
            var defaultInstance = Activator.CreateInstance(t);
            var pi = t.GetProperties();
            var fi = t.GetFields();
            DataRow row;
            object columnvalue;
            DbObject.GetEmptyDataTable();
            DbObject.DataTable.Rows.Clear();
            foreach (var myclass in List)
            {
                row = DbObject.DataTable.NewRow();
                foreach (PropertyInfo prop in pi)
                {
                    try
                    {
                        // Get the value of the row according to the field name
                        // Remember that the classïs members and the tableïs field names
                        // must be identical
                        //columnvalue = Utilities.CallByName(myclass, prop.Name, Utilities.CallType.Get);
                        columnvalue = Interaction.CallByName(myclass, prop.Name, CallType.Get);
                        // Know check if the value is null. 
                        // If not, it will be added to the instance
                        if (!ReferenceEquals(columnvalue, DBNull.Value))
                        {
                            row[prop.Name] = columnvalue;
                        }
                    }
                    catch
                    {
                        // Console.WriteLine(prop.Name & ": " & ex.ToString())
                        // Return Nothing
                    }
                }

                foreach (FieldInfo prop in fi)
                {
                    try
                    {
                        // Get the value of the row according to the field name
                        // Remember that the classïs members and the tableïs field names
                        // must be identical
                        // Dim columnvalue As Object = CallByName([myclass], prop.Name, CallType.Get)
                        //columnvalue = Utilities.CallByName(myclass, prop.Name, Utilities.CallType.Get);
                        columnvalue = Interaction.CallByName(myclass, prop.Name, CallType.Get);
                        // Know check if the value is null. 
                        // If not, it will be added to the instance
                        if (!ReferenceEquals(columnvalue, DBNull.Value))
                        {
                            if (!ReferenceEquals(columnvalue, DBNull.Value))
                            {
                                row[prop.Name] = columnvalue;
                            }
                        }
                    }
                    catch
                    {
                        // Console.WriteLine(prop.Name & ": " & ex.ToString())
                        // Return Nothing
                    }
                    // Threading.Thread.Sleep(0)
                }

                DbObject.DataTable.Rows.Add(row);
                // Threading.Thread.Sleep(0)
            }

            DbObject.DataTable.AcceptChanges();
            return default;
        }
    }

    public class ExecutionResult
    {
        public object Value = null;
        public eResultCode ResultCode = 0;
        public int LastDllError = 0;
        public int ErrorCode = 0;
        public string ResultMessage = "";
        public Exception Exception = null;
        public string Context = "";
        public InnerExecutionResult InnerExecutionResult;
        private bool mFailed;

        public bool Success
        {
            get
            {
                if (ResultCode == eResultCode.Failed)
                {
                    return false;
                }
                else
                {
                    return true;
                }
            }
            // set
            // {
            // mFailed = value;
            // }
        }

        public bool Failed
        {
            get
            {
                if (ResultCode == eResultCode.Failed)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            // set
            // {
            // mFailed = value;
            // }
        }

        public void Reset()
        {
            Value = null;
            // Failed = false;
            ResultCode = eResultCode.Success;
            ErrorCode = 0;
            ResultMessage = "";
            Exception = null;
            Information.Err().Clear();
        }

        public InnerExecutionResult ToInnerExecutionResult()
        {
            var x = new InnerExecutionResult();
            x.Context = Context;
            x.ErrorCode = ErrorCode;
            x.Exception = Exception;
            x.ResultCode = (InnerExecutionResult.eResultCode)ResultCode;
            x.ResultMessage = ResultMessage;
            x.Value = Value;
            return x;
        }

        public ExecutionResult Set(ExecutionResult ExecutionResult)
        {
            ResultCode = ExecutionResult.ResultCode;
            Exception = ExecutionResult.Exception;
            InnerExecutionResult = ExecutionResult.ToInnerExecutionResult();
            return this;
        }


        public ExecutionResult(string Context = "")
        {
            Reset();
            this.Context = Context;
        }

        public ExecutionResult GetFromDb(DbObject DbObject)
        {
            return GetFromDbObject(DbObject);
        }

        public ExecutionResult GetFromDb(DbConfig DbConfig)
        {
            return GetFromDbConfig(DbConfig);
        }

        public ExecutionResult GetFromDbObject(DbObject DbObject)
        {
            if (!string.IsNullOrEmpty(DbObject.LastError))
            {
                ResultCode = eResultCode.Failed;
            }
            else
            {
                ResultCode = eResultCode.Success;
            }

            ErrorCode = DbObject.LastErrorCode;
            ResultMessage = DbObject.LastErrorComplete;
            Exception = DbObject.LastErrorException;
            return this;
        }

        public ExecutionResult GetFromDbConfig(DbConfig DbConfig)
        {
            if (!string.IsNullOrEmpty(DbConfig.LastError))
            {
                ResultCode = eResultCode.Failed;
            }
            else
            {
                ResultCode = eResultCode.Success;
            }

            ErrorCode = DbConfig.LastErrorCode;
            ResultMessage = DbConfig.LastErrorException.Message;
            Exception = DbConfig.LastErrorException;
            return this;
        }

        public enum eResultCode
        {
            Success = 0,
            Warning = 1,
            Failed = 2
        }
    }

    public class InnerExecutionResult
    {
        public object Value = null;
        public eResultCode ResultCode = 0;
        public int ErrorCode = 0;
        public string ResultMessage = "";
        public Exception Exception = null;
        public string Context = "";
        private bool mFailed;

        public bool Success
        {
            get
            {
                if (ResultCode == eResultCode.Failed)
                {
                    return false;
                }
                else
                {
                    return true;
                }
            }
            // set
            // {
            // mFailed = value;
            // }
        }

        public bool Failed
        {
            get
            {
                if (ResultCode == eResultCode.Failed)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            // set
            // {
            // mFailed = value;
            // }
        }

        public void Reset()
        {
            Value = null;
            // Failed = false;
            ResultCode = eResultCode.Success;
            ErrorCode = 0;
            ResultMessage = "";
            Exception = null;
        }

        public InnerExecutionResult(string Context = "")
        {
            Reset();
            this.Context = Context;
        }

        public enum eResultCode
        {
            Success = 0,
            Warning = 1,
            Failed = 2
        }
    }

    public sealed class Validators
    {
        public Validators()
        {
        }

        public static bool Required(object Value)
        {
            bool Validation = false;
            if (Value is object)
            {
                Validation = true;
            }

            return Validation;
        }

        public static bool ValidEmail(string Value, bool AllowNull = false)
        {
            bool Validation = false;
            if (string.IsNullOrEmpty(Value) & AllowNull == true)
            {
                return true;
            }

            string pattern;
            pattern = @"^([0-9a-zA-Z]([-\.\w]*[0-9a-zA-Z])*@([0-9a-zA-Z][-\w]*[0-9a-zA-Z]\.)+[a-zA-Z]{2,9})$";
            if (System.Text.RegularExpressions.Regex.IsMatch(Value, pattern))
            {
                Validation = true;
            }

            return Validation;
        }

        public static bool ValidURL(string Value, bool AllowNull = false)
        {
            bool Validation = false;
            // If (Value = "" And AllowNull = True) Then
            // Return True
            // End If
            if (string.IsNullOrEmpty(Value))
            {
                return true;
            }

            string pattern;
            pattern = @"http(s)?://([\w+?\.\w+])+([a-zA-Z0-9\~\!\@\#\$\%\^\&\*\(\)_\-\=\+\\\/\?\.\:\;\'\,]*)?";
            if (System.Text.RegularExpressions.Regex.IsMatch(Value, pattern))
            {
                Validation = true;
            }

            return Validation;
        }

        public static bool RegExp(object Value, string Pattern, bool AllowNull = false)
        {
            bool Validation = false;
            // If (Value = "" And AllowNull = True) Then
            // Return True
            // End If

            if (Convert.ToString(Value) == "")
            {
                return true;
            }

            if (System.Text.RegularExpressions.Regex.IsMatch(Convert.ToString(Value), Pattern))
            {
                Validation = true;
            }

            return Validation;
        }

        public static bool ValidURI(string Value, bool AllowNull = false)
        {
            bool Validation = false;
            // If (Value = "" And AllowNull = True) Then
            // Return True
            // End If
            if (string.IsNullOrEmpty(Value))
            {
                return true;
            }

            if (Uri.IsWellFormedUriString(Value, UriKind.Absolute))
            {
                Validation = true;
            }

            return Validation;
        }

        public static bool NotEqual(object Value, object ValidValue, bool AllowNull = false)
        {
            bool Validation = false;
            // If (Value.ToString() = "" And AllowNull = True) Then
            // Return True
            // End If
            if (string.IsNullOrEmpty(Value.ToString()))
            {
                return true;
            }

            try
            {
                Value = Convert.ChangeType(Value, ValidValue.GetType());
            }
            catch
            {
                return false;
            }

            if (Value != ValidValue)
            {
                Validation = true;
            }

            return Validation;
        }

        public static bool Equal(object Value, object ValidValue, bool AllowNull = false)
        {
            bool Validation = false;
            // If (Value.ToString() = "" And AllowNull = True) Then
            // Return True
            // End If
            if (string.IsNullOrEmpty(Value.ToString()))
            {
                return true;
            }

            try
            {
                Value = Convert.ChangeType(Value, ValidValue.GetType());
            }
            catch
            {
                return false;
            }

            if (Value == ValidValue)
            {
                Validation = true;
            }

            return Validation;
        }

        public static bool GreaterThan(object Value, object ValidValue, bool AllowNull = false)
        {
            bool Validation = false;
            // If (Value.ToString() = "" And AllowNull = True) Then
            // Return True
            // End If
            if (string.IsNullOrEmpty(Value.ToString()))
            {
                return true;
            }

            try
            {
                Value = Convert.ChangeType(Value, ValidValue.GetType());
            }
            catch
            {
                return false;
            }

            //if (Value > ValidValue)
            //{
            //    Validation = true;
            //}

            return Validation;
        }

        public static bool GreaterThanOrEqual(object Value, object ValidValue, bool AllowNull = false)
        {
            bool Validation = false;
            // If (Value.ToString() = "" And AllowNull = True) Then
            // Return True
            // End If
            if (string.IsNullOrEmpty(Value.ToString()))
            {
                return true;
            }

            try
            {
                Value = Convert.ChangeType(Value, ValidValue.GetType());
            }
            catch
            {
                return false;
            }

            //if (Conversions.ToBoolean(Operators.ConditionalCompareObjectGreaterEqual(Value, ValidValue, false)))
            //{
            //    Validation = true;
            //}

            return Validation;
        }

        public static bool LessThan(object Value, object ValidValue, bool AllowNull = false)
        {
            bool Validation = false;
            // If (Value.ToString() = "" And AllowNull = True) Then
            // Return True
            // End If
            if (string.IsNullOrEmpty(Value.ToString()))
            {
                return true;
            }

            try
            {
                Value = Convert.ChangeType(Value, ValidValue.GetType());
            }
            catch
            {
                return false;
            }

            //if (Conversions.ToBoolean(Operators.ConditionalCompareObjectLess(Value, ValidValue, false)))
            //{
            //    Validation = true;
            //}

            return Validation;
        }

        public static bool LessThanOrEqual(object Value, object ValidValue, bool AllowNull = false)
        {
            bool Validation = false;
            // If (Value.ToString() = "" And AllowNull = True) Then
            // Return True
            // End If
            if (string.IsNullOrEmpty(Value.ToString()))
            {
                return true;
            }

            try
            {
                Value = Convert.ChangeType(Value, ValidValue.GetType());
            }
            catch
            {
                return false;
            }

            //if (Conversions.ToBoolean(Operators.ConditionalCompareObjectLessEqual(Value, ValidValue, false)))
            //{
            //    Validation = true;
            //}

            return Validation;
        }

        public static bool CompareList(object Value, object ValidationList, bool AllowNull = false)
        {
            bool Validation = false;
            // If (Value.ToString() = "" And AllowNull = True) Then
            // Return True
            // End If
            if (string.IsNullOrEmpty(Value.ToString()))
            {
                return true;
            }

            var type = ValidationList.GetType().GetGenericArguments()[0];
            IList ValidationCompareList = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(type));
            ValidationCompareList = (IList)ValidationList;
            try
            {
                if (ValidationCompareList.Contains(Value))
                {
                    Validation = true;
                }
            }
            catch
            {
            }

            return Validation;
        }

        public static object CompareListToString(object ValidationList)
        {
            var type = ValidationList.GetType().GetGenericArguments()[0];
            IList ValidationCompareList = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(type));
            ValidationCompareList = (IList)ValidationList;
            string s = "";
            //foreach (object _s in ValidationCompareList)
            //    s = Conversions.ToString(Operators.AddObject(Operators.AddObject(s, _s), Constants.vbCrLf));
            return s;
        }

        public static bool Range(object Value, object MinValue, object MaxValue, bool AllowNull = false)
        {

            // If (Value.ToString() = "" And AllowNull = True) Then
            // Return True
            // End If
            if (string.IsNullOrEmpty(Value.ToString()))
            {
                return true;
            }

            try
            {
                Value = Convert.ChangeType(Value, MinValue.GetType());
            }
            catch
            {
                return false;
            }

            if (Value.GetType() != MinValue.GetType())
            {
                return false;
            }

            if (Value.GetType() != MaxValue.GetType())
            {
                return false;
            }

            if (MaxValue.GetType() != MinValue.GetType())
            {
                return false;
            }

            return false;
            // return Conversions.ToBoolean(Operators.ConditionalCompareObjectGreaterEqual(Value, MinValue, false) && Operators.ConditionalCompareObjectLessEqual(Value, MaxValue, false));
        }
    }

    #region SerializableDictionary
    [XmlRoot("dictionary")]
    public class SerializableDictionary<TKey, TValue> : Dictionary<TKey, TValue>, IXmlSerializable
    {

        #region IXmlSerializable Members

        public System.Xml.Schema.XmlSchema GetSchema()
        {
            return null;
        }

        public void ReadXml(System.Xml.XmlReader reader)
        {
            var keySerializer = new XmlSerializer(typeof(TKey));
            var valueSerializer = new XmlSerializer(typeof(TValue));
            bool wasEmpty = reader.IsEmptyElement;
            reader.Read();
            if (wasEmpty)
            {
                return;
            }

            while (reader.NodeType != System.Xml.XmlNodeType.EndElement)
            {
                reader.ReadStartElement("item");
                reader.ReadStartElement("key");
                TKey key = (TKey)keySerializer.Deserialize(reader);
                reader.ReadEndElement();
                reader.ReadStartElement("value");
                TValue value = (TValue)valueSerializer.Deserialize(reader);
                reader.ReadEndElement();
                Add(key, value);
                reader.ReadEndElement();
                reader.MoveToContent();
            }

            reader.ReadEndElement();
        }

        public void WriteXml(System.Xml.XmlWriter writer)
        {
            var keySerializer = new XmlSerializer(typeof(TKey));
            var valueSerializer = new XmlSerializer(typeof(TValue));
            foreach (TKey key in Keys)
            {
                writer.WriteStartElement("item");
                writer.WriteStartElement("key");
                keySerializer.Serialize(writer, key);
                writer.WriteEndElement();
                writer.WriteStartElement("value");
                var value = this[key];
                valueSerializer.Serialize(writer, value);
                writer.WriteEndElement();
                writer.WriteEndElement();
            }
        }
        #endregion
    }

    #endregion


    public sealed class Utilities
    {
        [System.Runtime.InteropServices.DllImport("advapi32.dll", SetLastError = true)]
        private static extern bool OpenProcessToken(IntPtr ProcessHandle, uint DesiredAccess, out IntPtr TokenHandle);
        [System.Runtime.InteropServices.DllImport("kernel32.dll", SetLastError = true)]
        [return: System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.Bool)]
        private static extern bool CloseHandle(IntPtr hObject);


        public static string GetBasicDALPublicKeyToken()
        {
            return GetPublicKeyTokenFromAssembly(System.Reflection.Assembly.GetExecutingAssembly());
        }

        public static string GetPublicKeyTokenFromAssembly(Assembly assembly)
        {
            var bytes = assembly.GetName().GetPublicKeyToken();
            if (bytes == null || bytes.Length == 0)
                return "None";

            var publicKeyToken = string.Empty;
            for (int i = 0; i < bytes.GetLength(0); i++)
                publicKeyToken += string.Format("{0:x2}", bytes[i]);

            return publicKeyToken;
        }

        static Assembly CurrentDomain_AssemblyResolve(object sender, ResolveEventArgs args)
        {

            //AppDomain.CurrentDomain.AssemblyResolve += CurrentDomain_AssemblyResolve;
            using (var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream("EmbedAssembly.EmbeddedAssemblies.Newtonsoft.Json.dll"))
           {
               var assemblyData = new Byte[stream.Length];
               stream.Read(assemblyData, 0, assemblyData.Length);
                return Assembly.Load(assemblyData);
            }
        }



        public static class StringHelper
        {

            public static string SequenceFrom(string value)
            {
                char chr;
                int i = 0;
                int c = 0;
                for (i = value.Length; i >= 1; i--)
                {
                    c = Convert.ToInt32(value[i - 1]);
                    if ((c < 48 ? true : c > 57))
                    {
                        if ((c < 65 ? false : c <= 90))
                        {
                            if ((c < 65 ? false : c <= 89))
                            {
                                c++;
                                string str = value.Remove(i - 1, 1);
                                chr = (char)c;
                                value = str.Insert(i - 1, chr.ToString());
                                break;
                            }
                            else if (c == 90)
                            {
                                c = 65;
                                string str1 = value.Remove(i - 1, 1);
                                chr = (char)c;
                                value = str1.Insert(i - 1, chr.ToString());
                            }
                        }
                    }
                    else if ((c < 48 ? false : c <= 56))
                    {
                        c++;
                        string str2 = value.Remove(i - 1, 1);
                        chr = (char)c;
                        value = str2.Insert(i - 1, chr.ToString());
                        break;
                    }
                    else if (c == 57)
                    {
                        c = 48;
                        string str3 = value.Remove(i - 1, 1);
                        chr = (char)c;
                        value = str3.Insert(i - 1, chr.ToString());
                    }
                }
                return value;
            }

            public static string SequenceFrom(string value, string SequenceMask)
            {
                char chr;
                bool flag;
                int x = 0;
                int i = 0;
                int c = 0;
                string a = null;
                x = value.Length;
                SequenceMask = SequenceMask.ToUpper();
                if (value.Length < SequenceMask.Length)
                {
                    value = new string(' ', SequenceMask.Length);
                }
                for (i = x; i >= 1; i--)
                {
                    a = SequenceMask.Substring(i - 1, 1);
                    if (a != "F")
                    {
                        c = Convert.ToInt32(value[i - 1]);
                        string str = a;
                        if (str != null)
                        {
                            if (str != "0")
                            {
                                goto Label2;
                            }
                            if ((c < 48 ? true : c > 56))
                            {
                                if (c != 57)
                                {
                                    c = 48;
                                    string str1 = value.Remove(i - 1, 1);
                                    chr = (char)c;
                                    value = str1.Insert(i - 1, chr.ToString());
                                }
                                else
                                {
                                    c = 48;
                                    string str2 = value.Remove(i - 1, 1);
                                    chr = (char)c;
                                    value = str2.Insert(i - 1, chr.ToString());
                                }
                                goto Label0;
                            }
                            else
                            {
                                c++;
                                string str3 = value.Remove(i - 1, 1);
                                chr = (char)c;
                                value = str3.Insert(i - 1, chr.ToString());
                                break;
                            }
                        }
                    Label2:;
                        flag = (c < 65 ? false : c <= 89);
                        if (flag)
                        {
                            c++;
                            string str4 = value.Remove(i - 1, 1);
                            chr = (char)c;
                            value = str4.Insert(i - 1, chr.ToString());
                            break;
                        }
                        else if (c != 90)
                        {
                            c = 65;
                            string str5 = value.Remove(i - 1, 1);
                            chr = (char)c;
                            value = str5.Insert(i - 1, chr.ToString());
                        }
                        else
                        {
                            c = 65;
                            string str6 = value.Remove(i - 1, 1);
                            chr = (char)c;
                            value = str6.Insert(i - 1, chr.ToString());
                        }
                    Label0:;
                    }
                }
                return value;
            }

        }
        public static class ProcessHelper
        {

            public static System.Diagnostics.Process GetCurrentProcess()
            {
                return System.Diagnostics.Process.GetCurrentProcess();
            }

            public static System.Diagnostics.Process GetProcessById(int processid)
            {
                return System.Diagnostics.Process.GetProcessById(processid );
            }


            public static string GetProcessUser(System.Diagnostics.Process process)
            {
                IntPtr processHandle = IntPtr.Zero;
                try
                {
                    OpenProcessToken(process.Handle, 8, out processHandle);
                    System.Security.Principal.WindowsIdentity wi = new System.Security.Principal.WindowsIdentity(processHandle);
                    string user = wi.Name;
                    return user.Contains(@"\") ? user.Substring(user.IndexOf(@"\") + 1) : user;
                }
                catch
                {
                    return null;
                }
                finally
                {
                    if (processHandle != IntPtr.Zero)
                    {
                        CloseHandle(processHandle);
                    }
                }
            }

        }



        public static void DBNullHandler(ref object value)
        {
            if (value is null)
            {
                value = DBNull.Value;
            }
        }
        public static string QuoteValue(string value)
        {
            return string.Concat("\"", value.Replace("\"", "\"\""), "\"");
        }

        public static bool IsNumeric(object Expression)
        {

            return Information.IsNumeric(Expression);
        }

        public static bool IsArray(object Expression)
        {

            return Expression is Array;
        }

        public static bool IsDate(object Expression)
        {
            return Expression is DateTime;
        }

        public static bool IsTime(object Expression)
        {
            return Expression is TimeSpan;
        }


        //public bool IsValidTimeFormat(string input)
        //{
        //    TimeSpan dummyOutput;
        //    return TimeSpan.TryParse(input, out dummyOutput);
        //}

        public static bool IsDBNull(object Expression)
        {


            return Expression is DBNull;
        }

        public static bool IsError(object Expression)
        {


            return Expression is Exception;
        }

        public static bool IsNothing(object Expression)
        {

            return Expression is null;
        }

        public static bool IsReference(object Expression)
        {


            return !(Expression is ValueType);
        }


        public class StreamHelper
        {
            public static byte[] streamToByteArray(Stream StreamIn)
            {
                if (StreamIn is object)
                {
                    using (var ms = new MemoryStream())
                    {
                        StreamIn.CopyTo(ms);
                        return ms.ToArray();
                    }
                }
                else
                {
                    return null;
                }
            }

            public static MemoryStream FileToMemoryStream(object FileName)
            {
                if (File.Exists(Convert.ToString(FileName)) == false)
                {
                    return null;
                }

                byte[] bData;
                BinaryReader br = new BinaryReader(File.OpenRead(Convert.ToString(FileName)));
                bData = br.ReadBytes((int)br.BaseStream.Length);
                MemoryStream ms = new MemoryStream();
                ms.Write(bData, 0, bData.Length);
                return ms;
            }

            public static FileStream FileToFileStream(object FileName)
            {
                if (File.Exists(Convert.ToString(FileName)) == false)
                {
                    return null;
                }

                FileStream ms = new FileStream(Convert.ToString(FileName), FileMode.Open);
                return ms;
            }
        }
        

        public class ByteArrayHelper
        {
            public static Stream byteArrayToStream(byte[] ByteIn)
            {
                Stream stream = null;
                if (ByteIn is object)
                {
                    stream = new MemoryStream(ByteIn);
                    return stream;
                }
                else
                {
                    return null;
                }
            }

            public static System.Drawing.Image byteArrayToImage(byte[] byteArrayIn)
            {
                System.Drawing.Image returnImage = null;
                if (byteArrayIn is object)
                {
                    var ms = new MemoryStream(byteArrayIn);
                    returnImage = System.Drawing.Image.FromStream(ms);
                    return returnImage;
                }

                return returnImage;
            }

            public static byte[] imageToByteArray(System.Drawing.Image imageIn, System.Drawing.Imaging.ImageFormat Format = null)
            {
                if (imageIn is object)
                {
                    var ms = new MemoryStream();
                    if (Format is null)
                    {
                        Format = System.Drawing.Imaging.ImageFormat.Jpeg;
                    }

                    imageIn.Save(ms, Format);
                    return ms.ToArray();
                }
                else
                {
                    return null;
                }
            }


        }

        public class SystemDrawingHelper
        {

            public static System.Drawing.Image SafeImageFromFile(string path)
            {
                byte[] bytesArr = null;
                if (File.Exists(path) == false)
                {
                    return null;
                }

                bytesArr = File.ReadAllBytes(path);
                MemoryStream memstr = new MemoryStream(bytesArr);
                System.Drawing.Image img = System.Drawing.Image.FromStream(memstr);
                return img;
            }

            public static byte[] imageToByteArray(System.Drawing.Image imageIn, System.Drawing.Imaging.ImageFormat Format = null)
            {
                if (imageIn is object)
                {
                    var ms = new MemoryStream();
                    if (Format is null)
                    {
                        Format = System.Drawing.Imaging.ImageFormat.Jpeg;
                    }

                    imageIn.Save(ms, Format);
                    return ms.ToArray();
                }
                else
                {
                    return null;
                }
            }

            public static System.Drawing.Image byteArrayToImage(byte[] byteArrayIn)
            {
                System.Drawing.Image returnImage = null;
                if (byteArrayIn is object)
                {
                    var ms = new MemoryStream(byteArrayIn);
                    returnImage = System.Drawing.Image.FromStream(ms);
                    return returnImage;
                }

                return returnImage;
            }


        }


        public static bool IsBase64String(string base64String)
        {
            base64String = base64String.Trim();
            return base64String.Length % 4 == 0 && System.Text.RegularExpressions.Regex.IsMatch(base64String, @"^[a-zA-Z0-9\+/]*={0,3}$", System.Text.RegularExpressions.RegexOptions.None);
        }



        public static class CryptographyHelper
        {


            public static string Encrypt(string stringtoencrypt)
            {
                return BasicDAL.Utilities.CryptographyHelper.EncryptTripleDES(stringtoencrypt, BasicDAL.Utilities.GetBasicDALPublicKeyToken(), true);
            }
            public static string Decrypt(string stringtodecrypt)
            {
                return BasicDAL.Utilities.CryptographyHelper.DecryptTripleDES(stringtodecrypt, BasicDAL.Utilities.GetBasicDALPublicKeyToken(), true);
            }


            private static System.Security.Cryptography.RijndaelManaged NewRijndaelManaged(string InputKey, string salt)
            {
                if (salt is null)
                {
                    throw new ArgumentNullException("salt");
                }

                byte[] saltBytes = Encoding.ASCII.GetBytes(salt);
                System.Security.Cryptography.Rfc2898DeriveBytes key = new System.Security.Cryptography.Rfc2898DeriveBytes(InputKey, saltBytes);
                System.Security.Cryptography.RijndaelManaged aesAlg = new System.Security.Cryptography.RijndaelManaged();
                aesAlg.Key = key.GetBytes(aesAlg.KeySize / 8);
                aesAlg.IV = key.GetBytes(aesAlg.BlockSize / 8);
                return (System.Security.Cryptography.RijndaelManaged)aesAlg;
            }

            public static string EncryptRijndael(string text, string salt, string InputKey = "BasicDAL")
            {
                if (string.IsNullOrEmpty(text))
                {
                    throw new ArgumentNullException("text");
                }

                System.Security.Cryptography.RijndaelManaged aesAlg = NewRijndaelManaged(InputKey, salt);
                System.Security.Cryptography.ICryptoTransform encryptor = aesAlg.CreateEncryptor(aesAlg.Key, aesAlg.IV);
                MemoryStream msEncrypt = new MemoryStream();
                using (Stream csEncrypt = new System.Security.Cryptography.CryptoStream((Stream)msEncrypt, (System.Security.Cryptography.ICryptoTransform)encryptor, System.Security.Cryptography.CryptoStreamMode.Write))
                {
                    using (StreamWriter swEncrypt = new StreamWriter(csEncrypt))
                    {
                        swEncrypt.Write(text);
                    }
                }

                return Convert.ToBase64String((byte[])msEncrypt.ToArray());
            }

            public static string DecryptRijndael(string cipherText, string salt, string InputKey = "BasicDAL")
            {
                if (string.IsNullOrEmpty(cipherText))
                {
                    throw new ArgumentNullException("cipherText");
                }
                if (!IsBase64String(cipherText))
                {
                    throw new Exception("The cipherText input parameter is not base64 encoded");
                }
                string text;
                System.Security.Cryptography.RijndaelManaged aesAlg = NewRijndaelManaged(InputKey, salt);
                System.Security.Cryptography.ICryptoTransform decryptor = aesAlg.CreateDecryptor(aesAlg.Key, aesAlg.IV);
                byte[] cipher = Convert.FromBase64String(cipherText);
                using (MemoryStream msDecrypt = new MemoryStream(cipher))
                {
                    using (Stream csDecrypt = new System.Security.Cryptography.CryptoStream((Stream)msDecrypt, (System.Security.Cryptography.ICryptoTransform)decryptor, System.Security.Cryptography.CryptoStreamMode.Read))
                    {
                        using (StreamReader srDecrypt = new StreamReader(csDecrypt))
                        {
                            text = Convert.ToString(srDecrypt.ReadToEnd());
                        }
                    }
                }
                return text;
            }

            public static string EncryptTripleDES(string toEncrypt, string key, bool useHashing)
            {
                byte[] keyArray;
                byte[] toEncryptArray = Encoding.UTF8.GetBytes(toEncrypt);

                // Dim settingsReader As System.Configuration.AppSettingsReader = New AppSettingsReader()
                // Get the key from config file

                // Dim key As String = DirectCast(settingsReader.GetValue("SecurityKey", GetType(String)), String)
                // System.Windows.Forms.MessageBox.Show(key);
                // If hashing use get hashcode regards to your key
                if (useHashing)
                {
                    var hashmd5 = new System.Security.Cryptography.MD5CryptoServiceProvider();
                    keyArray = hashmd5.ComputeHash(Encoding.UTF8.GetBytes(key));
                    // Always release the resources and flush data
                    // of the Cryptographic service provide. Best Practice

                    hashmd5.Clear();
                }
                else
                {
                    keyArray = Encoding.UTF8.GetBytes(key);
                }

                System.Security.Cryptography.TripleDESCryptoServiceProvider tdes = new System.Security.Cryptography.TripleDESCryptoServiceProvider();
                // set the secret key for the tripleDES algorithm
                tdes.Key = keyArray;
                // mode of operation. there are other 4 modes. We choose ECB(Electronic code Book)
                tdes.Mode = System.Security.Cryptography.CipherMode.ECB;
                // padding mode(if any extra byte added)
                tdes.Padding = System.Security.Cryptography.PaddingMode.PKCS7;
                System.Security.Cryptography.ICryptoTransform cTransform = tdes.CreateEncryptor();
                // transform the specified region of bytes array to resultArray
                byte[] resultArray = cTransform.TransformFinalBlock(toEncryptArray, 0, toEncryptArray.Length);
                // Release resources held by TripleDes Encryptor
                tdes.Clear();
                // Return the encrypted data into unreadable string format
                return Convert.ToBase64String(resultArray, 0, resultArray.Length);
            }

            public static string DecryptTripleDES(string cipherString, string key, bool useHashing)
            {
                byte[] keyArray;
                // get the byte code of the string

                byte[] toEncryptArray = Convert.FromBase64String(cipherString);

                // Dim settingsReader As System.Configuration.AppSettingsReader = New AppSettingsReader()
                // Get your key from config file to open the lock!
                // Dim key As String = DirectCast(settingsReader.GetValue("SecurityKey", GetType(String)), String)

                if (useHashing)
                {
                    // if hashing was used get the hash code with regards to your key
                    System.Security.Cryptography.MD5CryptoServiceProvider hashmd5 = new System.Security.Cryptography.MD5CryptoServiceProvider();
                    keyArray = hashmd5.ComputeHash(Encoding.UTF8.GetBytes(key));
                    // release any resource held by the MD5CryptoServiceProvider

                    hashmd5.Clear();
                }
                else
                {
                    // if hashing was not implemented get the byte code of the key
                    keyArray = Encoding.UTF8.GetBytes(key);
                }

                System.Security.Cryptography.TripleDESCryptoServiceProvider tdes = new System.Security.Cryptography.TripleDESCryptoServiceProvider();
                // set the secret key for the tripleDES algorithm
                tdes.Key = keyArray;
                // mode of operation. there are other 4 modes.
                // We choose ECB(Electronic code Book)

                tdes.Mode = System.Security.Cryptography.CipherMode.ECB;
                // padding mode(if any extra byte added)
                tdes.Padding = System.Security.Cryptography.PaddingMode.PKCS7;
                System.Security.Cryptography.ICryptoTransform cTransform = tdes.CreateDecryptor();
                byte[] resultArray = cTransform.TransformFinalBlock(toEncryptArray, 0, toEncryptArray.Length);
                // Release resources held by TripleDes Encryptor
                tdes.Clear();
                // return the Clear decrypted TEXT
                return Encoding.UTF8.GetString(resultArray);
            }

            public static string GetHashValue(string SourceText, HashProviders HashProvider = HashProviders.MD5)
            {
                var Ue = new UnicodeEncoding();
                var ByteSourceText = Ue.GetBytes(SourceText);
                byte[] ByteHash = null;
                switch (HashProvider)
                {
                    case HashProviders.MD5:
                        {
                            System.Security.Cryptography.MD5CryptoServiceProvider Md5 = new System.Security.Cryptography.MD5CryptoServiceProvider();
                            ByteHash = Md5.ComputeHash(ByteSourceText);
                            break;
                        }

                    case HashProviders.SHA256:
                        {
                            System.Security.Cryptography.SHA256CryptoServiceProvider SHA256 = new System.Security.Cryptography.SHA256CryptoServiceProvider();
                            ByteHash = SHA256.ComputeHash(ByteSourceText);
                            break;
                        }

                    case HashProviders.SHA384:
                        {
                            System.Security.Cryptography.SHA384CryptoServiceProvider SHA384 = new System.Security.Cryptography.SHA384CryptoServiceProvider();
                            ByteHash = SHA384.ComputeHash(ByteSourceText);
                            break;
                        }

                    case HashProviders.SHA512:
                        {
                            System.Security.Cryptography.SHA512CryptoServiceProvider SHA512 = new System.Security.Cryptography.SHA512CryptoServiceProvider();
                            ByteHash = SHA512.ComputeHash(ByteSourceText);
                            break;
                        }

                    default:
                        {
                            System.Security.Cryptography.MD5CryptoServiceProvider Md5 = new System.Security.Cryptography.MD5CryptoServiceProvider();
                            ByteHash = Md5.ComputeHash(ByteSourceText);
                            break;
                        }
                }

                return Convert.ToBase64String(ByteHash);
            }

            public static string GetHashValue(byte[] ByteSourceText, HashProviders HashProvider = HashProviders.MD5)
            {
                byte[] ByteHash = null;
                switch (HashProvider)
                {
                    case HashProviders.MD5:
                        {
                            System.Security.Cryptography.MD5CryptoServiceProvider Md5 = new System.Security.Cryptography.MD5CryptoServiceProvider();
                            ByteHash = Md5.ComputeHash(ByteSourceText);
                            break;
                        }

                    case HashProviders.SHA256:
                        {
                            System.Security.Cryptography.SHA256CryptoServiceProvider SHA256 = new System.Security.Cryptography.SHA256CryptoServiceProvider();
                            ByteHash = SHA256.ComputeHash(ByteSourceText);
                            break;
                        }

                    case HashProviders.SHA384:
                        {
                            System.Security.Cryptography.SHA384CryptoServiceProvider SHA384 = new System.Security.Cryptography.SHA384CryptoServiceProvider();
                            ByteHash = SHA384.ComputeHash(ByteSourceText);
                            break;
                        }

                    case HashProviders.SHA512:
                        {
                            System.Security.Cryptography.SHA512CryptoServiceProvider SHA512 = new System.Security.Cryptography.SHA512CryptoServiceProvider();
                            ByteHash = SHA512.ComputeHash(ByteSourceText);
                            break;
                        }

                    default:
                        {
                            System.Security.Cryptography.MD5CryptoServiceProvider Md5 = new System.Security.Cryptography.MD5CryptoServiceProvider();
                            ByteHash = Md5.ComputeHash(ByteSourceText);
                            break;
                        }
                }

                return Convert.ToBase64String(ByteHash);
            }
        }

             

        public static string GetControlFullName(dynamic Control, string Prefix = "")
        {
            var cnames = new List<string>();
            string _c = "";
            dynamic _Control = Control;
            _c = Convert.ToString(Control.Name);
            do
            {
                if (_Control.Parent is object)
                {
                    if (_Control.Name != Control.Parent.Name)
                    {
                        _c = Convert.ToString(_Control.Parent.Name & "." + _c);
                        _Control = _Control.Parent;
                    }
                    else
                    {
                        break;
                    }
                }
                else
                {
                    break;
                }
            }
            while (true);
            return Prefix + _c;
        }

        public static class SerializationHelper
        {
            public static void DeserializeObjectFromXMLString(string XMLString, ref object Object)
            {
                //object returnObject = null;
                if (string.IsNullOrEmpty(XMLString))
                {
                    Object = null;
                }

                try
                {
                    var srReader = new StringReader(XMLString);
                    var tType = Object.GetType();
                    var xsSerializer = new XmlSerializer(tType);
                    var oData = xsSerializer.Deserialize(srReader);
                    srReader.Close();
                    Object = oData;
                }
                catch
                {
                }
            }

            public static string SerializeObjectToXMLString(object Object)
            {
                try
                {
                    var swWriter = new StringWriter();
                    var tType = Object.GetType();
                    if (tType.IsSerializable)
                    {
                        var xsSerializer = new XmlSerializer(tType);
                        xsSerializer.Serialize(swWriter, Object);
                        return swWriter.ToString();
                    }
                }
                catch
                {
                }

                return null;
            }

            public static string SimpleSerialize<T>(T toSerialize)
            {
                var xmlSerializer = new XmlSerializer(toSerialize.GetType());
                using (var textWriter = new StringWriter())
                {
                    try
                    {
                        xmlSerializer.Serialize(textWriter, toSerialize);
                        return textWriter.ToString();
                    }
                    catch
                    {
                        return "";
                    }
                }
            }

            public static T SimpleDeserialize<T>(string XMLString)
            {
                T returnObject = default;
                if (string.IsNullOrEmpty(XMLString))
                {
                    return default;
                }

                try
                {
                    var xmlStream = new StringReader(XMLString);
                    var serializer = new XmlSerializer(typeof(T));
                    returnObject = (T)serializer.Deserialize(xmlStream);
                }
                catch
                {
                }

                return returnObject;
            }


            public static bool DeserializeObjectFromFile(string FileName, ref object Object)
            {
                bool ok = false;
                try
                {
                    if (File.Exists(FileName))
                    {
                        var srReader = File.OpenText(FileName);
                        var tType = Object.GetType();
                        var xsSerializer = new XmlSerializer(tType);
                        var oData = xsSerializer.Deserialize(srReader);
                        srReader.Close();
                        Object = oData;
                        ok = true;
                    }
                }
                catch
                {
                }

                return ok;
            }
            public static bool SerializeObjectToFile(string FileName, object Object)
            {
                bool ok = false;
                try
                {
                    var swWriter = File.CreateText(FileName);
                    var tType = Object.GetType();
                    if (tType.IsSerializable)
                    {
                        var xsSerializer = new XmlSerializer(tType);
                        xsSerializer.Serialize(swWriter, Object);
                        swWriter.Close();
                        ok = true;
                    }
                }
                catch
                {
                }
    
                return ok;
            }
        }

        public static class FileHelper
        {

            public static MemoryStream FileToMemoryStream(object FileName)
            {
                if (File.Exists(Convert.ToString(FileName)) == false)
                {
                    return null;
                }

                byte[] bData;
                BinaryReader br = new BinaryReader(File.OpenRead(Convert.ToString(FileName)));
                bData = br.ReadBytes((int)br.BaseStream.Length);
                MemoryStream ms = new MemoryStream();
                ms.Write(bData, 0, bData.Length);
                return ms;
            }

            public static FileStream FileToFileStream(object FileName)
            {
                if (File.Exists(Convert.ToString(FileName)) == false)
                {
                    return null;
                }

                FileStream ms = new FileStream(Convert.ToString(FileName), FileMode.Open);
                return ms;
            }

            public static string GetFileName(string Path)
            {
                return System.IO.Path.GetFileName(Path);
            }

            public static string GetUniqueFileName(string FileName)
            {
                string _Path = FileName;
                string _FileName = GetFileNameWithoutExtension(FileName);
                string _FileNameComplete = GetFileName(FileName);
                string _Extension = GetFileExtension(FileName);
                if (FileName.EndsWith(_FileNameComplete))
                {
                    _Path = FileName.Substring(0, FileName.Length - _FileNameComplete.Length - 1);
                }

                string _NewFileName = _Path + _FileName + _Extension;
                if (System.IO.File.Exists(_NewFileName))
                {
                    int i = 1;
                    do
                    {
                        string s = "_" + i;
                        _NewFileName = _Path + _FileName + s + _Extension;
                        if (System.IO.File.Exists(_NewFileName) == false)
                        {
                            break;
                        }

                        i = i + 1;
                        if (i > 2000)
                        {
                            break;
                        }
                    }
                    while (true);
                }

                return _NewFileName;
            }

            public static string GetFileExtension(string UserInput)
            {
                return System.IO.Path.GetExtension(UserInput);
            }

            public static string GetFileNameWithoutExtension(string UserInput)
            {
                return System.IO.Path.GetFileNameWithoutExtension(UserInput);
            }

            public static string GetSafeFileName(string UserInput)
            {
                foreach (char invalidChar in Path.GetInvalidFileNameChars())
                    UserInput = UserInput.Replace(Convert.ToString(invalidChar), "");
                return UserInput;
            }

        }


     

        
        public static object RegisterEventHandler(dynamic EventObject, object EventAddressOf)
        {
            EventObject.RemoveEventHandler(EventObject, EventAddressOf);
            EventObject.AddHandler(EventObject, EventAddressOf);
            return EventObject;
        }

    
        public static string GetURLWithoutFileName(string url)
        {
            if (url.StartsWith("http://") == false)
            {
                return url;
            }
            else
            {
                //url = Strings.Mid(url, 8, url.Length);
                url = url.Substring(7, url.Length);
            }

            //int last_i = Strings.InStrRev(url, "/");
            int last_i = url.LastIndexOf("/");
            int first_i = url.IndexOf("/");
            if (last_i >= first_i & last_i > 0)
            {
                return "http://" + url.Substring(0, last_i - 1);
            }
            else
            {
                return "http://" + url + "/";
            }
        }


        public static  class DataTableHelper
        {


            public static DataRow CloneDataRow(DataRow dataRow)
            {
                DataRow dr;
                DataTable dtNew;
                dtNew = dataRow.Table.Clone();
                dr = dtNew.NewRow();
                dr.ItemArray = dataRow.ItemArray;
                dtNew.Rows.Add(dr);
                return dtNew.Rows[0];
            }

            public static string DataTableToJson(DataTable table)
            {
                string JSONString = string.Empty;
                JSONString = JsonConvert.SerializeObject(table, Formatting.None);
                return JSONString;
            }

            public static string DataRowToJson(DataRow dataRow )
            {
                string JSONString = string.Empty;
                JSONString = JsonConvert.SerializeObject(dataRow, Formatting.None);
                return JSONString;
            }

            
            public static List<T> DataTableToList<T>(DataTable table) where T : class, new()
            {
                try
                {
                    var list = new List<T>();
                    var proplist = new List<PropertyInfo>();
                    var _obj = new T();
                    foreach (PropertyInfo prop in _obj.GetType().GetProperties())
                        // MsgBox(prop.Name & " - " & prop.PropertyType.ToString())
                        proplist.Add(prop);
                    foreach (DataRow row in table.AsEnumerable())
                    {
                        var obj = new T();
                        foreach (PropertyInfo prop in proplist)
                        {
                            try
                            {
                                Type propType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                                object safevalue = null;
                                if (IsDBNull(row[prop.Name]) == true)
                                {
                                }
                                else
                                {
                                    safevalue = row[prop.Name] is null ? null : Convert.ChangeType(row[prop.Name], propType);
                                }

                                prop.SetValue(obj, safevalue, null);
                            }
                            catch
                            {
                                continue;
                            }
                        }

                        list.Add(obj);
                    }

                    return list;
                }
                catch
                {
                    return null;
                }
            }

            public static DataTable DataTableRemoveDuplicatesRows(DataTable SourceTable)
            {
                EnumerableRowCollection<DataRow> UniqueRows = (EnumerableRowCollection<DataRow>)SourceTable.AsEnumerable().Distinct(DataRowComparer.Default);
                if (UniqueRows is object)
                {
                    try
                    {
                        DataTable DestinatioTable;
                        DestinatioTable = (DataTable)UniqueRows.CopyToDataTable();
                        return DestinatioTable;
                    }
                    catch
                    {
                        return SourceTable;
                    }
                }
                else
                {
                    return SourceTable;
                }
            }

            public static DataTable DataTableRemoveDuplicatesRows(DataTable SourceTable, string ColumnName)
            {
                object _value = null;
                DataTable DestinationTable;
                DestinationTable = SourceTable.Clone();
                for (int i = 0, loopTo = SourceTable.Rows.Count - 1; i <= loopTo; i++)
                {
                    DataRow sr = SourceTable.Rows[i];
                    if (sr[ColumnName] != _value)
                    {
                        DestinationTable.ImportRow(sr);
                    }

                    _value = SourceTable.Rows[i][ColumnName];
                }
    
                return DestinationTable;
            }

            public static IList GetIListFromDataTable(DataTable DataTable)
            {
                IList IList = null;
                try
                {
                    IList = DataTable.Select().ToList();
                }
                catch
                {
                }

                return IList;
            }

            public static IList GetListFromTable(DataTable Table, ICopy DummyObj, IList ObjList)
            {
                foreach (DataRow dr in Table.Rows)
                    ObjList.Add(DummyObj.NewCopy(dr.ItemArray));
                return ObjList;
            }
            public static DataTable IListToDataTable<T>(IList<T> iList)
            {
                var dataTable = new DataTable();
                var propertyDescriptorCollection = TypeDescriptor.GetProperties(typeof(T));
                for (int i = 0, loopTo = propertyDescriptorCollection.Count - 1; i <= loopTo; i++)
                {
                    var propertyDescriptor = propertyDescriptorCollection[i];
                    dataTable.Columns.Add(propertyDescriptor.Name, propertyDescriptor.PropertyType);
                }

                var values = new object[propertyDescriptorCollection.Count];
                foreach (T iListItem in iList)
                {
                    for (int i = 0, loopTo1 = values.Length - 1; i <= loopTo1; i++)
                        values[i] = propertyDescriptorCollection[i].GetValue(iListItem);
                    dataTable.Rows.Add(values);
                }

                return dataTable;
            }

            public static T[] DataTableToArray<T>(DataTable dataTable)
            {
                var tType = typeof(T);
                var tPropertiesInfo = tType.GetProperties();
                return DataTableToArray<T>(dataTable, tType, tPropertiesInfo, GetColumnIndices(tPropertiesInfo, dataTable.Columns.Cast<DataColumn>().ToArray()));
            }


            private static int[] GetColumnIndices(PropertyInfo[] tPropertiesInfo, DataColumn[] dataColumns)
            {
                PropertyInfo tPropertyInfo;
                DataColumn dataColumn;
                var columnIndicesMappings = new int[(tPropertiesInfo.Count())];
                for (int i = 0, loopTo = tPropertiesInfo.Count() - 1; i <= loopTo; i++)
                {
                    tPropertyInfo = tPropertiesInfo[i];
                    for (int j = 0, loopTo1 = dataColumns.Count() - 1; j <= loopTo1; j++)
                    {
                        dataColumn = dataColumns[j];
                        if ((tPropertyInfo.Name ?? "") == (dataColumn.ColumnName ?? ""))
                        {
                            columnIndicesMappings[i] = j;
                            break;
                        }
                    }
                }

                return columnIndicesMappings;
            }

            private static T[] DataTableToArray<T>(DataTable dataTable, Type tType, PropertyInfo[] tPropertiesInfo, int[] columnIndices)
            {
                DataRow dataRow;
                T tInstance;
                var array = new T[dataTable.Rows.Count];
                for (int i = 0, loopTo = dataTable.Rows.Count - 1; i <= loopTo; i++)
                {
                    dataRow = dataTable.Rows[i];
                    tInstance = (T)Activator.CreateInstance(tType);
                    for (int j = 0, loopTo1 = tPropertiesInfo.Count() - 1; j <= loopTo1; j++)
                        tPropertiesInfo[j].SetValue(tInstance, dataRow[columnIndices[j]], null);
                    array[i] = tInstance;
                }

                return array;
            }
        }


        public object GetDbValue(object DbValue)
        {

            if (IsDBNull(DbValue))
            {
                return null;
            }
            else
            {
                return DbValue;
            }
        }


        public static int DbObject_GetTotalROWSCOUNT(DbObject DBOjbect)
        {
            string sqlCount = "SELECT COUNT(*) FROM " + DBOjbect.DbTableName;
            int RowCount;
            if (DBOjbect.DbObjectInitialized)
            {
                RowCount = Convert.ToInt32(DBOjbect.ExecuteScalar(sqlCount));
                if (!string.IsNullOrEmpty(DBOjbect.LastError))
                {
                    return -1;
                }
                else
                {
                    return RowCount;
                }
            }
            else
            {
                return -1;
            }
        }


        
        public interface ICopy
        {
            ICopy NewCopy(object[] @params);
        }

    
        public static class ParameterHelper
        {
            public static void SetParameter(ref dynamic Parameter, DbColumn DbColumn)
            {
                Parameter.DbType = DbColumn.DbType;
                Parameter.Size = DbColumn.Size;
                Parameter.Scale = DbColumn.NumericScale;
            }

            public static object ConvertParameterValue(DbColumn DbColumn, object Value = null)
            {
                object _value = DBNull.Value;
                //bool esito = false;
                try
                {
                    if (IsDBNull(Value) == false)
                    {
                        if (Value is null)
                        {
                            _value = DbValueCast(DbColumn.Value, DbColumn.DbType);
                        }
                        else
                        {
                            _value = Value;
                        }
                        // _value = DBNull.Value
                    }
                }
                catch
                {
                }

                if (IsDBNull(_value) == false)
                {


                    //if (DbColumn.DbColumnNameE == "ActionOnSourceLocationInventory")
                    //{
                    //    int zz = 0;

                    //}
                    switch (DbColumn.DbType)
                    {
                        case DbType.AnsiString or DbType.AnsiStringFixedLength or DbType.String or DbType.StringFixedLength:
                            {
                                if (DbColumn.Size > 0)
                                {
                                    if (_value.ToString().Length >= DbColumn.Size)
                                    {
                                        try
                                        {
                                            _value = Convert.ToString(_value).Substring(0, DbColumn.Size);
                                        }
                                        catch
                                        {
                                        }
                                    }
                                }
                                break;
                            }

                        case DbType.Time:
                            {
                                switch (DbColumn.DbObject.DbConfig.Provider)
                                {
                                    case Providers.OracleDataAccess:

#if COMPILEORACLE
                                        TimeSpan ts = default;
                                        ts = Conversions.ToDate(DbColumn.Value).TimeOfDay;
                                        if (ts != null)
                                        {
                                            var OracleInterval = new Oracle.ManagedDataAccess.Types.OracleIntervalDS(ts.Days, ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
                                            _value = OracleInterval.ToString();
                                        }
#endif


                                        break;

                                    default:
                                        {
                                            break;
                                        }
                                }

                                break;
                            }

                        default:
                            {
                                break;
                            }
                    }
                }

                return _value;
            }

        }
     
        
        public static object DbValueCast(object Value, DbType dbType)
            {
                object CastRet = default;
                try
                {
                    switch (dbType)
                    {
                        case DbType.AnsiString or DbType.AnsiStringFixedLength or DbType.String or DbType.StringFixedLength:
                            {
                                CastRet = Convert.ToString(Value);
                                break;
                            }

                        case DbType.Byte:
                            {
                                CastRet = Convert.ToByte(Value);
                                break;
                            }

                        case DbType.Boolean:
                            {
                                CastRet = Convert.ToBoolean(Value);
                                break;
                            }

                        case DbType.Currency or DbType.Decimal or DbType.VarNumeric:
                            {
                                CastRet = Convert.ToDecimal(Value);
                                break;
                            }

                        case DbType.Single:
                            {
                                CastRet = Convert.ToSingle(Value);
                                break;
                            }

                        case DbType.Date or DbType.DateTime:
                            {
                                if (Value is null == false)
                                {
                                    CastRet = Convert.ToDateTime(Value);
                                }
                                else
                                {
                                    CastRet = DBNull.Value;
                                }

                                break;
                            }

                        case DbType.Time:
                            {
                                if (Value == null)
                                {
                                    CastRet = TimeSpan.Parse("00:00:00");
                                }
                                //CastRet = (Timespan)Convert.ToDateTime(Value);
                                CastRet= TimeSpan.Parse(Value.ToString());
                            break;
                            }

                        case DbType.Double:
                            {
                                CastRet = Convert.ToDouble(Value);
                                break;
                            }

                        case DbType.Guid:
                            {
                                CastRet = Convert.ToString(Value);
                                break;
                            }

                        case DbType.SByte:
                            {
                                CastRet = Convert.ToSByte(Value);
                                break;
                            }

                        case DbType.Int16:
                            {
                                CastRet = Convert.ToInt16(Value);
                                break;
                            }

                        case DbType.Int32:
                            {
                                CastRet = Convert.ToInt32(Value);
                                break;
                            }

                        case DbType.Int64:
                            {
                                CastRet = Convert.ToInt64(Value);
                                break;
                            }

                        case DbType.UInt16:
                            {
                                CastRet = Convert.ToUInt16(Value);
                                break;
                            }

                        case DbType.UInt32:
                            {
                                CastRet = Convert.ToUInt32(Value);
                                break;
                            }

                        case DbType.UInt64:
                            {
                                CastRet = Convert.ToUInt64(Value);
                                break;
                            }

                        default:
                            {
                                CastRet = Value;
                                break;
                            }
                    }
                }
                catch
                {
                    return null;
                } // DBNull.Value

                return CastRet;
            }


            public static object DbValueCast(object Value, DbType dbType, DateTime NullDateValue, DateTimeResolution DateTimeResolution)
            {
                try
                {
                    switch (dbType)
                    {
                        case DbType.AnsiString:
                        case DbType.AnsiStringFixedLength:
                        case DbType.String:
                        case DbType.StringFixedLength:
                            {
                                return Convert.ToString(Value);
                            }

                        case DbType.Byte:
                            {
                                return Convert.ToByte(Value);
                            }

                        case DbType.Boolean:
                            {
                                return Convert.ToBoolean(Value);
                            }

                        case DbType.Currency:
                        case DbType.Decimal:
                        case DbType.VarNumeric:
                            {
                                return Convert.ToDecimal(Value, System.Globalization.CultureInfo.InvariantCulture);
                            }

                        case DbType.Single:
                            {
                                return Convert.ToSingle(Value, System.Globalization.CultureInfo.InvariantCulture);
                            }

                        case DbType.Date:
                        case DbType.DateTime:
                        case DbType.DateTime2:
                            {
                                if (Conversions.ToBoolean(Operators.ConditionalCompareObjectEqual(Value, NullDateValue, false)))
                                {
                                    Value = null;
                                }

                                if (Value is null == false)
                                {
                                    return TruncateDateTime(Conversions.ToDate(Value), DateTimeResolution);
                                }
                                else
                                {
                                    return DBNull.Value;
                                }
                            }

                        case DbType.Time:
                            {
                                if (Value == null)
                                {
                                    Value = "00:00:00";
                                }
                            //return Convert.ToDateTime(Value);
                            return TimeSpan.Parse(Value.ToString());
                        }
                        case DbType.Double:
                            {
                                return Convert.ToDouble(Value, System.Globalization.CultureInfo.InvariantCulture);
                            }

                        case DbType.Guid:
                            {
                                return Convert.ToString(Value);
                            }

                        case DbType.SByte:
                            {
                                return Convert.ToSByte(Value);
                            }

                        case DbType.Int16:
                            {
                                return Convert.ToInt16(Value);
                            }

                        case DbType.Int32:
                            {
                                return Convert.ToInt32(Value);
                            }

                        case DbType.Int64:
                            {
                                return Convert.ToInt64(Value);
                            }

                        case DbType.UInt16:
                            {
                                return Convert.ToUInt16(Value);
                            }

                        case DbType.UInt32:
                            {
                                return Convert.ToUInt32(Value);
                            }

                        case DbType.UInt64:
                            {
                                return Convert.ToUInt64(Value);
                            }

                        default:
                            {
                                return Value;
                            }
                    }
                }
                catch
                {
                    return null;
                } // DBNull.Value
            }


            public static object DbValueCast(object Value, DbType dbType, DateTimeResolution DateTimeResolution)
            {
                try
                {
                    switch (dbType)
                    {
                        case DbType.AnsiString or DbType.AnsiStringFixedLength or DbType.String or DbType.StringFixedLength:
                            {
                                return Convert.ToString(Value);

                            }

                        case DbType.Byte:
                            {
                                return Convert.ToByte(Value);
                            }

                        case DbType.Boolean:
                            {
                                return Convert.ToBoolean(Value);
                            }

                        case DbType.Currency or DbType.Decimal or DbType.VarNumeric:
                            {
                                return Convert.ToDecimal(Value);
                            }

                        case DbType.Single:
                            {
                                return Convert.ToSingle(Value);
                            }

                        case DbType.Date or DbType.DateTime or DbType.DateTime2:
                            {
                                if (Value is null == false)
                                {
                                    return Utilities.TruncateDateTime(Convert.ToDateTime(Value), DateTimeResolution);
                                }
                                else
                                {
                                    return DBNull.Value;
                                }
                            }

                        case DbType.Time:
                            {
                                if (Value == null)
                                {
                                    Value = "00:00:00";
                                }
                                return Convert.ToDateTime(Value);
                            }
                        case DbType.Double:
                            {
                                return Convert.ToDouble(Value);
                            }

                        case DbType.Guid:
                            {
                                return Convert.ToString(Value);
                            }

                        case DbType.SByte:
                            {
                                return Convert.ToSByte(Value);
                            }

                        case DbType.Int16:
                            {
                                return Convert.ToInt16(Value);
                            }

                        case DbType.Int32:
                            {
                                return Convert.ToInt32(Value);
                            }

                        case DbType.Int64:
                            {
                                return Convert.ToInt64(Value);
                            }

                        case DbType.UInt16:
                            {
                                return Convert.ToUInt16(Value);
                            }

                        case DbType.UInt32:
                            {
                                return Convert.ToUInt32(Value);
                            }

                        case DbType.UInt64:
                            {
                                return Convert.ToUInt64(Value);
                            }

                        default:
                            {
                                return Value;
                            }
                    }
                }
                catch
                {
                    return null;
                } // DBNull.Value
            }

        

        public static DateTime TruncateDateTime(DateTime Date, DateTimeResolution resolution = DateTimeResolution.Second)
        {
            switch (resolution)
            {
                case DateTimeResolution.Year:
                    {
                        return new DateTime(Date.Year, 1, 1, 0, 0, 0, 0, Date.Kind);
                    }

                case DateTimeResolution.Month:
                    {
                        return new DateTime(Date.Year, Date.Month, 1, 0, 0, 0, Date.Kind);
                    }

                case DateTimeResolution.Day:
                    {
                        return new DateTime(Date.Year, Date.Month, Date.Day, 0, 0, 0, Date.Kind);
                    }

                case DateTimeResolution.Hour:
                    {
                        return Date.AddTicks(-(Date.Ticks % TimeSpan.TicksPerHour));
                    }

                case DateTimeResolution.Minute:
                    {
                        return Date.AddTicks(-(Date.Ticks % TimeSpan.TicksPerMinute));
                    }

                case DateTimeResolution.Second:
                    {
                        return Date.AddTicks(-(Date.Ticks % TimeSpan.TicksPerSecond));
                    }

                case DateTimeResolution.Millisecond:
                    {
                        return Date.AddTicks(-(Date.Ticks % TimeSpan.TicksPerMillisecond));
                    }

                case DateTimeResolution.Tick:
                    {
                        return Date.AddTicks(0L);
                    }

                default:
                    {
                        throw new ArgumentException("BasicDAL.Utilities.TruncateDateTime. Unrecognized resolution", "resolution");
                    }
            }
        }

        public enum CallType
        {
            /// <summary>
            /// Gets a value from a property.
            /// </summary>
            Get,
            /// <summary>
            /// Sets a value into a property.
            /// </summary>
            Let,
            /// <summary>
            /// Invokes a method.
            /// </summary>
            Method,
            /// <summary>
            /// Sets a value into a property.
            /// </summary>
            Set
        }



        /// <summary>
        /// Allows late bound invocation of
        /// properties and methods.
        /// </summary>
        /// <param name="target">Object implementing the property or method.</param>
        /// <param name="methodName">Name of the property or method.</param>
        /// <param name="callType">Specifies how to invoke the property or method.</param>
        /// <param name="args">List of arguments to pass to the method.</param>
        /// <returns>The result of the property or method invocation.</returns>
        public static object _CallByName(object target, string methodName, CallType callType, params object[] args)
        {

            //xfi = GetType().GetField(mField.Name, BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Public | BindingFlags.Static);
            //dbcolumn = (DbColumn)xfi.GetValue(this);
            //BindingFlags binding = BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Public | BindingFlags.Static;

            switch (callType)
            {
                case CallType.Get:
                    {
                        //FieldInfo   p = target.GetType().GetField(methodName,binding );
                        //return p.GetValue(target);
                        return Interaction.CallByName(target, methodName, Microsoft.VisualBasic.CallType.Get, args);
                    }
                case CallType.Let:
                    return Interaction.CallByName(target, methodName, Microsoft.VisualBasic.CallType.Let, args);
                case CallType.Set:
                    {
                        //FieldInfo p = target.GetType().GetField (methodName, binding );
                        //p.SetValue(target, args[0]);
                        return Interaction.CallByName(target, methodName, Microsoft.VisualBasic.CallType.Set, args);
                        //eturn null;
                    }
                case CallType.Method:
                    {
                        //MethodInfo m = target.GetType().GetMethod(methodName,binding );
                        //return m.Invoke(target, args);
                        return Interaction.CallByName(target, methodName, Microsoft.VisualBasic.CallType.Method, args);
                    }
            }

            return null;
        }



        public static object CreateDBTableFromDBObject(DbObject DBObject, string TableName, Providers Provider)
        {
            string sqlsc;
            sqlsc = "CREATE TABLE " + TableName + "(";
            foreach (DbColumn DbColumn in DBObject.GetDbColumns())
            {
                string s = "";
                switch (DbColumn.DataTypeName.ToLower() ?? "")
                {
                    case "varchar":
                    case "nvarchar":
                    case "char":
                    case "nchar":
                        {
                            s = DbColumn.DataTypeName + "(" + DbColumn.Size + ")";
                            break;
                        }

                    default:
                        {
                            break;
                        }
                }

                sqlsc = sqlsc + "\n" + "[" + DbColumn.Name + "] " + s;
                if (DbColumn.IsAutoIncrement)
                {
                    sqlsc += " IDENTITY(0,1) ";
                }

                if (!DbColumn.AllowDBNull)
                {
                    sqlsc += " NOT NULL ";
                }

                sqlsc += ",";
            }

            return sqlsc;
        }

        public static string Reflection_GetValidFilename(string Filename)
        {
            string regex = string.Format("[{0}]", System.Text.RegularExpressions.Regex.Escape(new string(Path.GetInvalidFileNameChars())));
            var removeInvalidChars = new System.Text.RegularExpressions.Regex(regex, System.Text.RegularExpressions.RegexOptions.Singleline | System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.CultureInvariant);
            return regex;
        }
        // Public Shared Sub Reflection_GetFields(ByVal t As Type)
        // Console.WriteLine("***** Fields *****")
        // Dim fi() As Reflection.FieldInfo = t.GetFields()
        // For Each field As Reflection.FieldInfo In fi
        // Console.WriteLine("->{0}", field.Name)
        // Next field
        // Console.WriteLine("")
        // End Sub

        // ' Display property names of type. 
        // Public Sub Reflection_GetProperties(ByVal t As Type)
        // Console.WriteLine("***** Properties *****")
        // Dim pi() As Reflection.PropertyInfo = t.GetProperties()
        // For Each prop As Reflection.PropertyInfo In pi
        // Console.WriteLine("->{0}", prop.Name)
        // Next prop
        // Console.WriteLine("")
        // End Sub

        public static string GetStringRightValue(string input, string separator = "=")
        {
            if (string.IsNullOrEmpty(separator))
            {
                separator = "=";
            }

            var sep = input.Split(Convert.ToChar(separator));
            switch (sep.Count())
            {
                case 2:
                    {
                        return sep[1];
                    }

                default:
                    {
                        return input;
                    }
            }
        }

        public static string GetStringLeftValue(string input, string separator = "=")
        {
            if (string.IsNullOrEmpty(separator))
            {
                separator = "=";
            }

            var sep = input.Split(Convert.ToChar(separator));
            switch (sep.Count())
            {
                case 2:
                    {
                        return sep[0];
                    }

                default:
                    {
                        return input;
                    }
            }
        }

        public static string EnumDescription(Enum EnumConstant)
        {
            var fi = EnumConstant.GetType().GetField(EnumConstant.ToString());
            DescriptionAttribute[] aattr = (DescriptionAttribute[])fi.GetCustomAttributes(typeof(DescriptionAttribute), false);
            if (aattr.Length > 0)
            {
                return aattr[0].Description;
            }
            else
            {
                return EnumConstant.ToString();
            }
        }

        public static string GetSQLCreateTable(string tableName, DataTable table)
        {
            string sqlsc;
            sqlsc = "CREATE TABLE " + tableName + "(";
            for (int i = 0, loopTo = table.Columns.Count - 1; i <= loopTo; i++)
            {
                sqlsc += "\n" + " [" + table.Columns[i].ColumnName + "] ";
                string columnType = table.Columns[i].DataType.ToString();
                switch (columnType ?? "")
                {
                    case "System.Int32":
                        {
                            sqlsc += " int ";
                            break;
                        }

                    case "System.Int64":
                        {
                            sqlsc += " bigint ";
                            break;
                        }

                    case "System.Int16":
                        {
                            sqlsc += " smallint";
                            break;
                        }

                    case "System.Byte":
                        {
                            sqlsc += " tinyint";
                            break;
                        }

                    case "System.Decimal":
                        {
                            sqlsc += " decimal ";
                            break;
                        }

                    case "System.DateTime":
                        {
                            sqlsc += " datetime ";
                            break;
                        }

                    case "System.Boolean":
                        {
                            sqlsc += " bit ";
                            break;
                        }

                    default:
                        {
                            sqlsc += string.Format(" nvarchar({0}) ", table.Columns[i].MaxLength == -1 ? "max" : table.Columns[i].MaxLength.ToString());
                            break;
                        }
                }

                if (table.Columns[i].AutoIncrement)
                {
                    sqlsc += " IDENTITY(" + table.Columns[i].AutoIncrementSeed.ToString() + "," + table.Columns[i].AutoIncrementStep.ToString() + ") ";
                }

                if (!table.Columns[i].AllowDBNull)
                {
                    sqlsc += " NOT NULL ";
                }

                sqlsc += ",";
            }

            return sqlsc.Substring(0, sqlsc.Length - 1) + "\n" + ")";
        }

        public static DataTable ConvertTo<T>(List<T> list)
        {
            var table = CreateTable<T>();
            var entityType = typeof(T);
            var properties = TypeDescriptor.GetProperties(entityType);
            foreach (T item in list)
            {
                var row = table.NewRow();
                foreach (PropertyDescriptor prop in properties)
                    row[prop.Name] = prop.GetValue(item);
                table.Rows.Add(row);
            }

            return table;
        }

        public static List<T> ConvertTo<T>(List<DataRow> rows)
        {
            List<T> list = null;
            if (rows is object)
            {
                list = new List<T>();
                foreach (DataRow row in rows)
                {
                    var item = CreateItem<T>(row);
                    list.Add(item);
                }
            }

            return list;
        }

        public static List<T> ConvertTo<T>(DataTable table)
        {
            if (table is null)
            {
                return null;
            }

            var rows = new List<DataRow>();
            foreach (DataRow row in table.Rows)
                rows.Add(row);
            return ConvertTo<T>(rows);
        }

        // Convert DataRow into T Object
        public static T CreateItem<T>(DataRow row)
        {
            string columnName;
            T obj = default;
            if (row is object)
            {
                obj = Activator.CreateInstance<T>();
                foreach (DataColumn column in row.Table.Columns)
                {
                    columnName = column.ColumnName;
                    // Get property with same columnName
                    var prop = obj.GetType().GetProperty(columnName);
                    try
                    {
                        // Get value for the column
                        var value = ReferenceEquals(row[columnName].GetType(), typeof(DBNull)) ? null : row[columnName];
                        // Set property value
                        prop.SetValue(obj, value, null);
                    }
                    catch
                    {
                        throw;
                        // Catch whatever here
                    }
                }
            }

            return obj;
        }

        public static DataTable CreateTable<T>()
        {
            var entityType = typeof(T);
            var table = new DataTable(entityType.Name);
            var properties = TypeDescriptor.GetProperties(entityType);
            foreach (PropertyDescriptor prop in properties)
                table.Columns.Add(prop.Name, prop.PropertyType);
            return table;
        }

        public static void HighlightChangedBoundControls(DbObject DbObject)
        {
            object DBValue = null;
            object EditedValue = null;
            foreach (DbColumn DbColumn in DbObject.GetDbColumns())
            {
                DBValue = DbValueCast(DbColumn.Value, DbColumn.DbType, DbColumn.DateTimeResolution);
                foreach (BoundControl BoundControl in DbColumn.BoundControls)
                {
                    EditedValue = DbValueCast(Interaction.CallByName(BoundControl.Control, BoundControl.PropertyName, Microsoft.VisualBasic.CallType.Get), DbColumn.DbType, DbColumn.DateTimeResolution);
                    // EditedValue = BasicDAL.Utilities.Cast(Microsoft.VisualBasic.Interaction.CallByName(BoundControl.Control, BoundControl.PropertyName, Microsoft.VisualBasic.CallType.Get), DbColumn.DbType, DbColumn.DateTimeResolution)
                    // qui fa la comparazione per i campi bound
                    if (!ReferenceEquals(DBValue, DBNull.Value) & !ReferenceEquals(EditedValue, DBNull.Value))
                    {
                        if (DBValue != EditedValue)
                        {
                            Interaction.CallByName(BoundControl.Control, "BackColor", Microsoft.VisualBasic.CallType.Set, (object)System.Drawing.Color.LightSalmon);
                        }
                        else
                        {
                            Interaction.CallByName(BoundControl.Control, "BackColor", Microsoft.VisualBasic.CallType.Set, (object)BoundControl.BackColor);
                        }
                    }

                    if (ReferenceEquals(DBValue, DBNull.Value) & !string.IsNullOrEmpty(EditedValue.ToString()))
                    {
                        Interaction.CallByName(BoundControl.Control, "BackColor", Microsoft.VisualBasic.CallType.Set, (object)System.Drawing.Color.LightSalmon);
                    }
                }
            }
        }
    }


    public class DbAuditing
    {
     
        System.Data.Common.DbCommand mAuditDbCommand;
        System.Data.Common.DbConnection mAuditDbConnection;

        System.Data.Common.DbParameter pID;
        System.Data.Common.DbParameter pTableName;
        System.Data.Common.DbParameter pRowUID;
        System.Data.Common.DbParameter pDateTime;
        System.Data.Common.DbParameter pProcessName;
        System.Data.Common.DbParameter pComputerName;
        System.Data.Common.DbParameter pUserName;
        System.Data.Common.DbParameter pProcessID;
        System.Data.Common.DbParameter pOperation;
        System.Data.Common.DbParameter pDataBefore;
        System.Data.Common.DbParameter pDataAfter;
        System.Data.Common.DbParameter pErrors;
        System.Data.Common.DbParameter pResults;
        System.Data.Common.DbParameter pCommand;
        System.Data.Common.DbParameter pProcessUserName;
        System.Data.Common.DbParameter pApplicationUserName;
        System.Data.Common.DbParameter pApplicationName;

        private DbObject mAuditDbObject;
        private System.Diagnostics.Process mAuditProcess;
        private System.Data.Common.DbProviderFactory mAuditDbProviderFactory;

        public DbAuditing()
        {
            mAuditProcess = System.Diagnostics.Process.GetCurrentProcess();

        }

        public DbAuditing(DbObject DbObject)
        {
            mAuditProcess = System.Diagnostics.Process.GetCurrentProcess();
            SetDbObject(DbObject);
            
        }

        public void SetDbObject(DbObject DbObject)
        {

            //this.mAuditDbObject = DbObject;
            //this.mAuditDbProviderFactory = this.mAuditDbObject.DbConfig.DbProviderFactory;

            //mAuditDbConnection = mAuditDbProviderFactory.CreateConnection();
            //mAuditDbConnection.ConnectionString = mAuditDbObject.DbConfig.DbConnection.ConnectionString;
            //mAuditDbCommand = mAuditDbProviderFactory.CreateCommand();
            //mAuditDbCommand.Connection = mAuditDbConnection;
            //mAuditDbConnection.Open();
            //InitParameters();
        
        }
    


        private void InitParameters()
        {

            if (mAuditDbObject == null)
                return;

            this.pID = this.mAuditDbProviderFactory.CreateParameter();
            this.pID.ParameterName = "@ID";
            this.pOperation = mAuditDbProviderFactory.CreateParameter();
            this.pOperation.ParameterName = "@Operation";
            this.pProcessID = mAuditDbProviderFactory.CreateParameter();
            this.pProcessID.ParameterName = "@ProcessID";
            this.pProcessName = mAuditDbProviderFactory.CreateParameter();
            this.pProcessName.ParameterName = "@ProcessName";
            this.pResults = mAuditDbProviderFactory.CreateParameter();
            this.pResults.ParameterName = "@Results";
            this.pRowUID = mAuditDbProviderFactory.CreateParameter();
            this.pRowUID.ParameterName = "@RowUID";
            this.pTableName = mAuditDbProviderFactory.CreateParameter();
            this.pTableName.ParameterName = "@TableName";
            this.pUserName = mAuditDbProviderFactory.CreateParameter();
            this.pUserName.ParameterName = "@UserName";
            this.pCommand = mAuditDbProviderFactory.CreateParameter();
            this.pCommand.ParameterName = "@Command";
            this.pComputerName = mAuditDbProviderFactory.CreateParameter();
            this.pComputerName.ParameterName = "@ComputerName";
            this.pDataAfter = mAuditDbProviderFactory.CreateParameter();
            this.pDataAfter.ParameterName = "@DataAfter";
            this.pDataBefore = mAuditDbProviderFactory.CreateParameter();
            this.pDataBefore.ParameterName = "@DataBefore";
            this.pDateTime = mAuditDbProviderFactory.CreateParameter();
            this.pDateTime.ParameterName = "@DateTime";
            this.pErrors = mAuditDbProviderFactory.CreateParameter();
            this.pErrors.ParameterName = "@Errors";
            this.pProcessUserName = mAuditDbProviderFactory.CreateParameter();
            this.pProcessUserName.ParameterName = "@ProcessUserName";
            this.pApplicationUserName = mAuditDbProviderFactory.CreateParameter();
            this.pApplicationUserName.ParameterName = "@ApplicationUserName";
            this.pApplicationName = mAuditDbProviderFactory.CreateParameter();
            this.pApplicationName.ParameterName = "@ApplicationName";

            this.mAuditDbCommand.Parameters.Clear();
            this.mAuditDbCommand.Parameters.Add(this.pID);
            this.mAuditDbCommand.Parameters.Add(this.pOperation);
            this.mAuditDbCommand.Parameters.Add(this.pProcessID);
            this.mAuditDbCommand.Parameters.Add(this.pResults);
            this.mAuditDbCommand.Parameters.Add(this.pRowUID);
            this.mAuditDbCommand.Parameters.Add(this.pTableName);
            this.mAuditDbCommand.Parameters.Add(this.pUserName);
            this.mAuditDbCommand.Parameters.Add(this.pCommand);
            this.mAuditDbCommand.Parameters.Add(this.pComputerName);
            this.mAuditDbCommand.Parameters.Add(this.pDataBefore);
            this.mAuditDbCommand.Parameters.Add(this.pDataAfter);
            this.mAuditDbCommand.Parameters.Add(this.pDateTime);
            this.mAuditDbCommand.Parameters.Add(this.pErrors);
            this.mAuditDbCommand.Parameters.Add(this.pProcessUserName);
            this.mAuditDbCommand.Parameters.Add(this.pProcessName);
            this.mAuditDbCommand.Parameters.Add(this.pApplicationUserName );
            this.mAuditDbCommand.Parameters.Add(this.pApplicationName);
        }

     

        public void WriteAudit(string Result, AuditOperations Operation)
        {

            //            System.Data.Common.DbParameter pParametersValues;



            string SQL = "INSERT INTO BasicDALAudit" +
                "(ID,TableName,RowUID,DateTime,ProcessName,ComputerName,UserName," +
                "ProcessID,Operation,DataBefore,DataAfter,Errors,Results,Command,ProcessUserName,ApplicationUserName,ApplicationName)" +
                "VALUES " +
                "(@ID,@TableName,@RowUID,@DateTime,@ProcessName,@ComputerName,@UserName," +
                "@ProcessID,@Operation,@DataBefore,@DataAfter,@Errors,@Results,@Command,@ProcessUserName,@ApplicationUserName,@ApplicationName)";


            this.pID.Value = 1;
            this.pTableName.Value = mAuditDbObject.DbTableName;
            this.pProcessID.Value = mAuditProcess.Id;
            this.pProcessName.Value = mAuditProcess.ProcessName;
            this.pComputerName.Value = "Computer";


            this.pUserName.Value = Utilities.ProcessHelper.GetProcessUser(mAuditProcess);
            this.pRowUID.Value = "@RowUID";
            this.pResults.Value = Result.ToString();

            switch (Operation)
            {
                case AuditOperations.BeforeInsert:
                    this.pOperation.Value = "BI";
                    break;
                case AuditOperations.AfterInsert:
                    this.pOperation.Value = "AI";
                    break;
                case AuditOperations.BeforeDelete:
                    this.pOperation.Value = "BD";
                    break;
                case AuditOperations.AfterDelete:
                    this.pOperation.Value = "AD";
                    break;
                case AuditOperations.AfterUpdate:
                    this.pOperation.Value = "AU";
                    this.pCommand.Value = mAuditDbObject.SQLUpdateStmt;
                    break;
                case AuditOperations.BeforeUpdate:
                    this.pOperation.Value = "BU";
                    this.pCommand.Value = mAuditDbObject.SQLUpdateStmt;
                    break;
                case AuditOperations.BeforeSelect:
                    this.pOperation.Value = "BS";
                    break;
                case AuditOperations.AfterSelect:
                    this.pOperation.Value = "AS";
                    break;

            }

            mAuditDbCommand.CommandText = SQL;

            try
            {
                mAuditDbCommand.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                if (mAuditDbObject.HandleErrors)
                {
                    if (mAuditDbObject.SuppressErrorsNotification == false)
                    {
                        if (mAuditDbObject.RedirectErrorsNotificationTo != null)
                        {
                            Interaction.CallByName(mAuditDbObject.RedirectErrorsNotificationTo, "Show", CallType.Method, "DbAuditing - \\n"+ex.Message );
                        }
                    }
                }
                else
                {
                    throw ex;
                }
            }


          



        }
    }
        public class ValidationRange
        {
            public object MinValue;
            public object MaxValue;
        }



    #region Attributes 

    [System.AttributeUsage(System.AttributeTargets.Property )]
    public class PocoPropertyAttributes : System.Attribute
    {
       public string DbColumnName { get; set; }
       
        
        public PocoPropertyAttributes()
        {

        }

    }


    [System.AttributeUsage(System.AttributeTargets.Property)]
    public class DbColumnAttributes: System.Attribute
    {
        
        public string Name{ get; set; }
        public object DefaultValue { get; set; }
        public string FriendlyName { get; set; }
        public string Description { get; set; }
        public string Format { get; set; }
        
        public System.Data.DbType DbType { get; set; }

        public bool IsIdentity { get; set; }
        public object IdentitySeed { get; set; }
        public DbColumnAttributes()
        {
            //this.ColumnName = ColumnName;

        }

    }
    [System.AttributeUsage(System.AttributeTargets.Class)]
    public class DbObjectAttributes : System.Attribute
    {
        
        public string Name { get; set; }
        public string FriendlyName { get; set; }
        
        
        public DbObjectAttributes()
        {
            //this.Name = Name;
            //this.FriendlyName = FriendlyName;
        }

    }

    #endregion





    #region Enumerations

    public enum DefaultValueEvaluationMode
        {
            OnEntityCreation = 0,
            OnInsert = 1,
            OnUpdate = 2
        }

        public enum DateTimeResolution
        {
            Year,
            Month,
            Day,
            Hour,
            Minute,
            Second,
            Millisecond,
            Tick
        }

        public enum DbColumnProperties
        {
            ColumnName = 0,
            ColumnOrdinal = 1,
            ColumnSize = 2,
            NumericPrecision = 3,
            NumericScale = 4,
            IsUnique = 5,
            IsKey = 6,
            BaseServerName = 7,
            BaseCatalogName = 8,
            BaseColumnName = 9,
            BaseSchemaName = 10,
            BaseTableName = 11,
            DataType = 12,
            AllowDBNull = 13,
            ProviderType = 14,
            IsAliased = 15,
            IsExpression = 16,
            IsIdentity = 17,
            IsAutoIncrement = 18,
            IsRowVersion = 19,
            IsHidden = 20,
            IsLong = 21,
            IsReadOnly = 22,
            ProviderSpecificDataType = 23,
            DataTypeName = 24,
            XmlSchemaCollectionDatabase = 25,
            XmlSchemaCollectionOwningSchema = 26,
            XmlSchemaCollectionName = 27,
            UdtAssemblyQualifiedName = 28,
            NonVersionedProviderType = 29,
            IsColumnSet = 30
        }

        public enum CryptoProviders
        {
            MD5 = 0,
            TripleDES = 1
        }

        public enum HashProviders
        {
            MD5 = 0,
            SHA256 = 1,
            SHA384 = 2,
            SHA512 = 3
        }

   
        public enum Providers
        {
            SqlServer,
            // SqlServerCe
            OleDb,
            OracleDataAccess,
            ODBC,
            //PostgreSQL,
            ConfigDefined,
            //Sharepoint,
            //Undefined,
            ODBC_DB2,
            OleDb_DB2,
            DB2_iSeries,
            MySQL
        }

        public enum LogicOperator
        {
            None = 0,
            AND = 1,
            OR = 2,
            NOT = 3
        }

        public enum ComparisionOperator
        {
            [Obsolete ("Use Equal")]    
            Equals = 0, // =
            Equal = 0, // =
            GreaterThan = 1, // >
            LessThan = 2, // <
            GreaterThanOrEqualTo = 3, // >=
            LessThanOrEqualTo = 4, // <=
            NotEqualTo = 5, // <>
            Between = 6,
            In = 7,
            Like = 8,
            NotLike = 9,
            Exists = 10,
            None = 11,
            IsNull = 12,
            IsNotNull = 13,
            NotIn = 14,
            NotBetween = 15
        }

        public enum ConnectionState
        {
            KeepOpen,
            CloseOnExit
        }

        public enum ParameterStyles
        {
            Simple = 0,
            Qualified = 1
        }

        public enum DataBoundControlsBehaviour
        {
            WindowsFormsDataBinding = 2,
            BasicDALDataBinding = 1,
            NoDataBinding = 0
        }

        public enum OrderBySortDirection
        {
            Ascendig = 0,
            Descending = 1
        }

        public enum AuditOperations
        {
            BeforeSelect=-4,    
            AfterSelect = 4,
            BeforeInsert = -1,
            AfterInsert=1,
            BeforeUpdate =-2,
            AfterUpdate = 2,
            BeforeDelete = -3,
            AfterDelete = 3
        }

        public enum InterfaceModeEnum
        {
            Public = 0,
            Property = 1,
            Private = 2
        }

        public enum DbObjectTypeEnum
        {
            Table = 0,
            View = 1,
            StoredProcedure = 2,
            Join = 3,
            InMemoryJoin = 4,
            SQLQuery = 5,
            TableFunction = 6,
            ScalarFunction = 7
        }

        public enum JoinType
        {
            InnerJoin = 0,
            LeftOuterJoin = 1,
            RightOuterJoin = 2,
            FullOuterJoin = 3
        }

        public enum BindingBehaviour
        {
            // READ = read the value from control property
            // WRITE = write the value of DbColumn.Value to the control property
            // ADDNEW = read the value from control property only if DbObject is adding a new row.
            // ReadWrite = True
            // Write = False
            // ReadWriteAddNew = 2
            // WriteAddNew = 3

            Read = 1,
            Write = 2,
            AddNew = 4,
            ReadWrite = 3,
            ReadWriteAddNew = 7,
            WriteAddNew = 6,
            ReadAddNew = 5
        }

        public enum ForeingKey
        {

            False = 0,
            True = 1,
            TrueNullable = 2
        }

        public enum Required
        {
            False = 0,
            True = 1,
            TrueNullable = 2
        }

        public enum RuntimeUI
        {
            Service = 0,
            WindowsForms = 1,
            Wisej = 2,
            Web = 3,
            WPF = 4
        }

        public enum DataEventType
        {
            Disposing = 0,
            Initializing = 1,
            AddNew = 2,
            Update = 3,
            Delete = 4,
            Query = 5,
            Insert = 6,
            UndoChanges = 9,
            Binding = 10,
            BindingFromDataReader = 11,
            ControlsBinding = 12,
            MoveFirst = 100,
            MovePrevious = 101,
            MoveLast = 102,
            MoveNext = 103,
            MoveTo = 104,
            AddToDataSet = 200,
            DeleteAll = 400,
            DeleteFromDataSet = 401,
            DeleteFromDataTable = 402,
            UpdateFromDataSet = 300,
            UpdateFromDataTable = 302
        }

        public enum ValidationTypes
        {
            None = 0,
            Required = 1,
            RegularExpression = 2,
            ValidURI = 6,
            Range = 3,
            CompareList = 4,
            Equal = 5,
            ValidURL = 7,
            ValidEmail = 8
        }

        public enum ValidationDataType
        {
            Default = 0,
            String = 1,
            Numeric = 2,
            DateTime = 3
        }

        public enum DataTypeFamily
        {
            String = 1,
            Numeric = 2,
            DateTime = 3,
            Time = 4,
            Boolean = 5,
            Binary = 6,
            XML = 7
        }

        public enum ACEApplyTo
        {
            DbConfig = 1,
            DbObject = 2,
            DbColumn = 3
        }

        public enum ACEAccessMask
        {
            None = 0,
            ReadExecute = 1,
            Write = 2,
            Delete = 4,
            Insert = 8,
            FullControl = 1024
        }

        public sealed class BuiltInSIDs
        {
            public const string Everyone = "everyone";
            public const string Administrators = "administrators";
        }

        public enum SIDTypes
        {
            User = 0,
            Group = 1
        }
    }


#endregion



