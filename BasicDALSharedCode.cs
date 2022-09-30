
#define COMPILEORACLE


using Microsoft.VisualBasic.CompilerServices;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace BasicDAL
{


    /* TODO ERROR: Skipped DefineDirectiveTrivia
    #Const COMPILEORACLE = False
    *//* TODO ERROR: Skipped DefineDirectiveTrivia
    #Const COMPILESQLCOMPACT = False
    */
   //static class BasicDalSharedCode
   // {
    //    public static bool IsNumeric(this DataColumn col)
    //    {
    //        if (col is null)
    //        {
    //            return false;
    //        }

    //        var TypeCode = Type.GetTypeCode(col.DataType);
    //        switch (TypeCode)
    //        {
    //            case System.TypeCode.Byte:
    //            case System.TypeCode.SByte:
    //            case System.TypeCode.Int16:
    //            case System.TypeCode.UInt16:
    //            case System.TypeCode.Int32:
    //            case System.TypeCode.UInt32:
    //            case System.TypeCode.Int64:
    //            case System.TypeCode.UInt64:
    //            case System.TypeCode.Single:
    //            case System.TypeCode.Double:
    //            case System.TypeCode.Decimal:
    //                {
    //                    return true;
    //                }

    //            default:
    //                {
    //                    return false;
    //                }
    //        }
    //    }

        //public static bool IsChanged(this DbColumn DbColumn)
        //{
        //    if (DbColumn is null)
        //    {
        //        return false;
        //    }

        //    if (Conversions.ToBoolean(Operators.ConditionalCompareObjectNotEqual(DbColumn.Value, DbColumn.OriginalValue, false)))
        //    {
        //        return true;
        //    }
        //    else
        //    {
        //        return false;
        //    }
        //}

        //public static bool IsBoolean(this DataColumn col)
        //{
        //    if (col is null)
        //    {
        //        return false;
        //    }

        //    var TypeCode = Type.GetTypeCode(col.DataType);
        //    switch (TypeCode)
        //    {
        //        case System.TypeCode.Boolean:
        //            {
        //                return true;
        //            }

        //        default:
        //            {
        //                return false;
        //            }
        //    }
        //}

        //public static bool IsString(this DataColumn col)
        //{
        //    if (col is null)
        //    {
        //        return false;
        //    }

        //    var TypeCode = Type.GetTypeCode(col.DataType);
        //    switch (TypeCode)
        //    {
        //        case System.TypeCode.String:
        //        case System.TypeCode.Char:
        //            {
        //                return true;
        //            }

        //        default:
        //            {
        //                return false;
        //            }
        //    }
        //}


        //public static IList GetIListFromDataTable(DataTable DataTable)
        //{
        //    IList IList = null;
        //    try
        //    {
        //        IList = DataTable.Select().ToList();
        //    }
        //    catch 
        //    {
        //    }

        //    return IList;
        //}

        //public static IList GetListFromTable(DataTable Table, ICopy DummyObj, IList ObjList)
        //{
        //    foreach (DataRow dr in Table.Rows)
        //        ObjList.Add(DummyObj.NewCopy(dr.ItemArray));
        //    return ObjList;
        //}

        //public interface ICopy
        //{
        //    ICopy NewCopy(object[] @params);
        //}

        //public static DataTable IListToDataTable<T>(this IList<T> iList)
        //{
        //    var dataTable = new DataTable();
        //    var propertyDescriptorCollection = System.ComponentModel.TypeDescriptor.GetProperties(typeof(T));
        //    for (int i = 0, loopTo = propertyDescriptorCollection.Count - 1; i <= loopTo; i++)
        //    {
        //        var propertyDescriptor = propertyDescriptorCollection[i];
        //        dataTable.Columns.Add(propertyDescriptor.Name, propertyDescriptor.PropertyType);
        //    }

        //    var values = new object[propertyDescriptorCollection.Count];
        //    foreach (T iListItem in iList)
        //    {
        //        for (int i = 0, loopTo1 = values.Length - 1; i <= loopTo1; i++)
        //            values[i] = propertyDescriptorCollection[i].GetValue(iListItem);
        //        dataTable.Rows.Add(values);
        //    }

        //    return dataTable;
        //}
    
        //public static T[] DataTableToArray<T>(this DataTable dataTable)
        //{
        //    var tType = typeof(T);
        //    var tPropertiesInfo = tType.GetProperties();
        //    return DataTableToArray<T>(dataTable, tType, tPropertiesInfo, GetColumnIndices(tPropertiesInfo, dataTable.Columns.Cast<DataColumn>().ToArray()));
        //}

        //private static int[] GetColumnIndices(System.Reflection.PropertyInfo[] tPropertiesInfo, DataColumn[] dataColumns)
        //{
        //    System.Reflection.PropertyInfo tPropertyInfo;
        //    DataColumn dataColumn;
        //    var columnIndicesMappings = new int[(tPropertiesInfo.Count())];
        //    for (int i = 0, loopTo = tPropertiesInfo.Count() - 1; i <= loopTo; i++)
        //    {
        //        tPropertyInfo = tPropertiesInfo[i];
        //        for (int j = 0, loopTo1 = dataColumns.Count() - 1; j <= loopTo1; j++)
        //        {
        //            dataColumn = dataColumns[j];
        //            if ((tPropertyInfo.Name ?? "") == (dataColumn.ColumnName ?? ""))
        //            {
        //                columnIndicesMappings[i] = j;
        //                break;
        //            }
        //        }
        //    }

        //    return columnIndicesMappings;
        //}

        //private static T[] DataTableToArray<T>(DataTable dataTable, Type tType, System.Reflection.PropertyInfo[] tPropertiesInfo, int[] columnIndices)
        //{
        //    DataRow dataRow;
        //    T tInstance;
        //    var array = new T[dataTable.Rows.Count];
        //    for (int i = 0, loopTo = dataTable.Rows.Count - 1; i <= loopTo; i++)
        //    {
        //        dataRow = dataTable.Rows[i];
        //        tInstance = (T)Activator.CreateInstance(tType);
        //        for (int j = 0, loopTo1 = tPropertiesInfo.Count() - 1; j <= loopTo1; j++)
        //            tPropertiesInfo[j].SetValue(tInstance, dataRow[columnIndices[j]], null);
        //        array[i] = tInstance;
        //    }

        //    return array;
        //}

        //public static void SetParameter(ref dynamic Parameter, DbColumn DbColumn)
        //{
        //    Parameter.DbType = DbColumn.DbType;
        //    Parameter.Size = DbColumn.Size;
        //    Parameter.Scale = DbColumn.NumericScale;
        //}

        //public static object _ConvertParameterValue(DbColumn DbColumn, object Value = null)
        //{
        //    object _value = DBNull.Value;
        //    //bool esito = false;
        //    try
        //    {
        //        if (Value is DBNull == false)
        //        {
        //            if (Value is null)
        //            {
        //                _value = Cast(DbColumn.Value, DbColumn.DbType);
        //            }
        //            else
        //            {
        //                _value = Value;
        //            }
        //            // _value = DBNull.Value
        //        }
        //    }
        //    catch 
        //    {
        //    }

        //    if (_value is DBNull == false)
        //    {

        //        if (DbColumn.DbColumnNameE == "ActionOnSourceLocationInventory")
        //        {
        //            int zz = 0;

        //        }    
        //        switch (DbColumn.DbType)
        //        {
        //            case DbType.AnsiString:
        //            case DbType.AnsiStringFixedLength:
        //            case DbType.String:
        //            case DbType.StringFixedLength:
        //                {
                            
        //                    if (DbColumn.Size > 0)
        //                    {
        //                        string _svalue = Convert.ToString(_value);  
        //                        if (_svalue.Length  > DbColumn.Size)
        //                        {
        //                            try
        //                            {
        //                                _value = _svalue.Substring (0, DbColumn.Size);
        //                            }
        //                            catch 
        //                            {
        //                            }
        //                        }
        //                    }

        //                    break;
        //                }

        //            case DbType.Time:
        //                {
        //                    switch (DbColumn.DbObject.DbConfig.Provider)
        //                    {
        //                        case Providers.OracleDataAccess:
        //                            TimeSpan ts = new TimeSpan();
        //                            ts = System.Convert.ToDateTime(DbColumn.Value).TimeOfDay;
        //                            if ((ts == new TimeSpan()) == false)
        //                            {

        //                                //var OracleInterval = new Oracle.DataAccess.Types.OracleIntervalDS(ts.Days, ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
        //                                //_value = OracleInterval.ToString();
        //                            }

        //                            break;

        //                        default:
        //                            {
        //                                break;
        //                            }
        //                    }

        //                    break;
        //                }

        //            default:
        //                {
        //                    break;
        //                }
        //        }
        //    }

        //    return _value;
        //}

        //public static object Cast(object Value, DbType dbType)
        //{
        //    object CastRet = default;
        //    try
        //    {
        //        switch (dbType)
        //        {
        //            case DbType.AnsiString:
        //            case DbType.AnsiStringFixedLength:
        //            case DbType.String:
        //            case DbType.StringFixedLength:
        //                {
        //                    CastRet = Convert.ToString(Value);
        //                    break;
        //                }

        //            case DbType.Byte:
        //                {
        //                    CastRet = Convert.ToByte(Value);
        //                    break;
        //                }

        //            case DbType.Boolean:
        //                {
        //                    CastRet = Convert.ToBoolean(Value);
        //                    break;
        //                }

        //            case DbType.Currency:
        //            case DbType.Decimal:
        //            case DbType.VarNumeric:
        //                {
        //                    CastRet = Convert.ToDecimal(Value, System.Globalization.CultureInfo.InvariantCulture);
        //                    break;
        //                }

        //            case DbType.Single:
        //                {
        //                    CastRet = Convert.ToSingle(Value, System.Globalization.CultureInfo.InvariantCulture);
        //                    break;
        //                }

        //            case DbType.Date:
        //            case DbType.DateTime:
        //                {
        //                    if (Value is null == false)
        //                    {
        //                        CastRet = Convert.ToDateTime(Value);
        //                    }
        //                    else
        //                    {
        //                        CastRet = DBNull.Value;
        //                    }

        //                    break;
        //                }

        //            case DbType.Time:
        //                {
        //                    CastRet = Convert.ToDateTime(Value);
        //                    break;
        //                }

        //            case DbType.Double:
        //                {
        //                    CastRet = Convert.ToDouble(Value, System.Globalization.CultureInfo.InvariantCulture);
        //                    break;
        //                }

        //            case DbType.Guid:
        //                {
        //                    CastRet = Convert.ToString(Value);
        //                    break;
        //                }

        //            case DbType.SByte:
        //                {
        //                    CastRet = Convert.ToSByte(Value);
        //                    break;
        //                }

        //            case DbType.Int16:
        //                {
        //                    CastRet = Convert.ToInt16(Value);
        //                    break;
        //                }

        //            case DbType.Int32:
        //                {
        //                    CastRet = Convert.ToInt32(Value);
        //                    break;
        //                }

        //            case DbType.Int64:
        //                {
        //                    CastRet = Convert.ToInt64(Value);
        //                    break;
        //                }

        //            case DbType.UInt16:
        //                {
        //                    CastRet = Convert.ToUInt16(Value);
        //                    break;
        //                }

        //            case DbType.UInt32:
        //                {
        //                    CastRet = Convert.ToUInt32(Value);
        //                    break;
        //                }

        //            case DbType.UInt64:
        //                {
        //                    CastRet = Convert.ToUInt64(Value);
        //                    break;
        //                }

        //            default:
        //                {
        //                    CastRet = Value;
        //                    break;
        //                }
        //        }
        //    }
        //    catch 
        //    {
        //        return null;
        //    } // DBNull.Value

        //    return CastRet;
        //}

        //public static object Cast(object Value, DbType dbType, DateTime NullDateValue, DateTimeResolution DateTimeResolution)
        //{
        //    try
        //    {
        //        switch (dbType)
        //        {
        //            case DbType.AnsiString:
        //            case DbType.AnsiStringFixedLength:
        //            case DbType.String:
        //            case DbType.StringFixedLength:
        //                {
        //                    return Convert.ToString(Value);
        //                }

        //            case DbType.Byte:
        //                {
        //                    return Convert.ToByte(Value);
        //                }

        //            case DbType.Boolean:
        //                {
        //                    return Convert.ToBoolean(Value);
        //                }

        //            case DbType.Currency:
        //            case DbType.Decimal:
        //            case DbType.VarNumeric:
        //                {
        //                    return Convert.ToDecimal(Value, System.Globalization.CultureInfo.InvariantCulture);
        //                }

        //            case DbType.Single:
        //                {
        //                    return Convert.ToSingle(Value, System.Globalization.CultureInfo.InvariantCulture);
        //                }

        //            case DbType.Date:
        //            case DbType.DateTime:
        //            case DbType.DateTime2:
        //                {
        //                    if (Conversions.ToBoolean(Operators.ConditionalCompareObjectEqual(Value, NullDateValue, false)))
        //                    {
        //                        Value = null;
        //                    }

        //                    if (Value is null == false)
        //                    {
        //                        return TruncateDateTime(Conversions.ToDate(Value), DateTimeResolution);
        //                    }
        //                    else
        //                    {
        //                        return DBNull.Value;
        //                    }
        //                }

        //            case DbType.Time:
        //                {
        //                    if (Value == null)
        //                    {
        //                        Value = "00:00:00";
        //                    }
        //                    return Convert.ToString(Value);
        //                }
        //            case DbType.Double:
        //                {
        //                    return Convert.ToDouble(Value, System.Globalization.CultureInfo.InvariantCulture);
        //                }

        //            case DbType.Guid:
        //                {
        //                    return Convert.ToString(Value);
        //                }

        //            case DbType.SByte:
        //                {
        //                    return Convert.ToSByte(Value);
        //                }

        //            case DbType.Int16:
        //                {
        //                    return Convert.ToInt16(Value);
        //                }

        //            case DbType.Int32:
        //                {
        //                    return Convert.ToInt32(Value);
        //                }

        //            case DbType.Int64:
        //                {
        //                    return Convert.ToInt64(Value);
        //                }

        //            case DbType.UInt16:
        //                {
        //                    return Convert.ToUInt16(Value);
        //                }

        //            case DbType.UInt32:
        //                {
        //                    return Convert.ToUInt32(Value);
        //                }

        //            case DbType.UInt64:
        //                {
        //                    return Convert.ToUInt64(Value);
        //                }

        //            default:
        //                {
        //                    return Value;
        //                }
        //        }
        //    }
        //    catch 
        //    {
        //        return null;
        //    } // DBNull.Value
        //}

        //public static DateTime TruncateDateTime(DateTime Date, DateTimeResolution resolution = DateTimeResolution.Second)
        //{
        //    switch (resolution)
        //    {
        //        case DateTimeResolution.Year:
        //            {
        //                return new DateTime(Date.Year, 1, 1, 0, 0, 0, 0, Date.Kind);
        //            }

        //        case DateTimeResolution.Month:
        //            {
        //                return new DateTime(Date.Year, Date.Month, 1, 0, 0, 0, Date.Kind);
        //            }

        //        case DateTimeResolution.Day:
        //            {
        //                return new DateTime(Date.Year, Date.Month, Date.Day, 0, 0, 0, Date.Kind);
        //            }

        //        case DateTimeResolution.Hour:
        //            {
        //                return Date.AddTicks(-(Date.Ticks % TimeSpan.TicksPerHour));
        //            }

        //        case DateTimeResolution.Minute:
        //            {
        //                return Date.AddTicks(-(Date.Ticks % TimeSpan.TicksPerMinute));
        //            }

        //        case DateTimeResolution.Second:
        //            {
        //                return Date.AddTicks(-(Date.Ticks % TimeSpan.TicksPerSecond));
        //            }

        //        case DateTimeResolution.Millisecond:
        //            {
        //                return Date.AddTicks(-(Date.Ticks % TimeSpan.TicksPerMillisecond));
        //            }

        //        case DateTimeResolution.Tick:
        //            {
        //                return Date.AddTicks(0L);
        //            }

        //        default:
        //            {
        //                throw new ArgumentException("unrecognized resolution", "resolution");
        //            }
        //    }
        //}
    //}
}