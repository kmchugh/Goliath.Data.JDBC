/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package Goliath.Data.JDBC;

import Goliath.Applications.Application;
import Goliath.Collections.HashTable;
import Goliath.Constants.LogType;
import Goliath.Exceptions.DataException;
import Goliath.Interfaces.Collections.ISimpleDataObjectCollection;
import Goliath.Interfaces.Data.ISimpleDataObject;
import Goliath.Interfaces.Data.IConnection;
import Goliath.Interfaces.Data.IConnectionString;
import Goliath.Interfaces.DynamicCode.ISqlGenerator;
import com.sun.rowset.CachedRowSetImpl;
import com.sun.rowset.internal.Row;
import java.io.ByteArrayInputStream;
import java.sql.ResultSetMetaData;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @param <T> 
 * @author kenmchugh
 */
public abstract class JDBCDataMapper<T extends ISimpleDataObject>// extends DataMapper<T, com.sun.rowset.internal.Row>
{
    // TODO: Remove this class once we are sure none of this code is needed
    /*
    private ISqlGenerator<T> m_oGenerator = null;
    ResultSetMetaData m_oMetaData = null;
    private String m_cKey = null;
    
    private JDBCQuery m_oUpdate;
    private JDBCQuery m_oInsert;
    private JDBCQuery m_oDelete;
    private JDBCQuery m_oRetrieve;
    
    private HashTable<Integer, JDBCMethod> m_oMethods = null;
    
    private class JDBCMethod
    {
        private String m_cColumnName;
        private Method m_oMethod;
        
    }
    
    protected ISqlGenerator<T> getGenerator(T toObject)
    {
        return getGenerator((Class<T>)toObject.getClass());
    }
    
    protected ISqlGenerator<T> getGenerator(Class<T> toClass)
    {
        if (m_oGenerator == null)
        {
            try
            {
                IConnectionString loString = DataManager.getInstance().getConnection(toClass);
                m_oGenerator = loString.getDataLayerAdapter().getQueryGenerator();
            }
            catch (Throwable ex)
            {
                
            }
        }
        return m_oGenerator;
    }
    
    protected IConnection getConnection(T toObject)
    {
        return getConnection((Class<T>)toObject.getClass());
    }
    
    protected IConnection getConnection(Class<T> toClass)
    {
        IConnectionString loString = DataManager.getInstance().getConnection(toClass);
        try
        {
            return ConnectionPool.getConnection(loString);
        }
        catch(Throwable ignore)
        {
            return null;
        }
    }
    
    @Override
    protected T onGetObjectByID(T toParameter) throws Goliath.Exceptions.Exception
    {
        return onGetObjectByKey(toParameter, getGenerator(toParameter).getKeyName(toParameter.getClass()));
    }
    
    private String getKey(T toObject)
    {
        if (m_cKey == null)
        {
            m_cKey = getGenerator(toObject).getKeyName(toObject.getClass());
        }
        return m_cKey;
    }
    
    private ResultSetMetaData getMetaData(Row toSource)
    {
        return m_oMetaData;
    }

    @Override
    protected void onCreateFromDataSource(T toObject, Row toSource) throws DataException
    {
        if (m_oMethods == null)
        {
            prepareMethods(toObject, getMetaData(toSource));
        }
        
        JDBCMethod loTemp = null;
        try
        {
            for (Integer loKey : m_oMethods.keySet())
            {
                loTemp = m_oMethods.get(loKey);
                loTemp.m_oMethod.invoke(toObject, new java.lang.Object[]{getGenerator(toObject).formatFromSQL(toSource.getColumnObject(loKey), loTemp.m_oMethod.getParameterTypes()[0])});
            }
        }
        catch (Throwable e)
        {
            throw new Goliath.Exceptions.DataException(e);
        }
    }
    
    private Class convertFromSQLClass(Class toClass)
    {
        if (java.sql.Timestamp.class.isAssignableFrom(toClass))
        {
            return Goliath.Date.class;
        }
        if (java.lang.Double.class.isAssignableFrom(toClass))
        {
            return java.lang.Float.class;
        }
        return toClass;
    }
    
    private void prepareMethods(T toObject, ResultSetMetaData toMetaData)
    {
        try
        {
            m_oMethods = new HashTable<Integer, JDBCMethod>();
            for (int i=1; i<= toMetaData.getColumnCount(); i++)
            {
                JDBCMethod loJDBCMethod = new JDBCMethod();
                String lcName = toMetaData.getColumnName(i);

                loJDBCMethod.m_cColumnName = lcName;
                if (lcName.equalsIgnoreCase(getKey(toObject)))
                {
                    lcName = "ID";
                }
                Method loMethod = null;
                Class loObjectClass = toObject.getClass();
                Class loValueClass = convertFromSQLClass(Class.forName(toMetaData.getColumnClassName(i)));
                Class loPrimitive = Goliath.DynamicCode.Java.getPrimitiveClass(loValueClass);

                // Check if there is a property as is
                try
                {
                    try
                    {
                        loMethod = loObjectClass.getMethod("set" + lcName, new Class[]{loValueClass});
                        
                    }
                    catch (Throwable ex)
                    {
                        if (loPrimitive != null)
                        {
                            // If the check is for a long, it may be a date field, so check for that also
                            try
                            {
                                loMethod = loObjectClass.getMethod("set" + lcName, new Class[]{loPrimitive});
                            }
                            catch(Throwable ex1)
                            {
                                if (loValueClass == Long.class)
                                {
                                    loMethod = loObjectClass.getMethod("set" + lcName, new Class[]{Goliath.Date.class});
                                }
                                else if (loValueClass == Integer.class || loValueClass == Short.class)
                                {
                                    try
                                    {
                                        loMethod = loObjectClass.getMethod("set" + lcName, new Class[]{boolean.class});
                                    }
                                    catch (Throwable ex2)
                                    {
                                        loMethod = loObjectClass.getMethod("set" + lcName, new Class[]{Boolean.class});
                                    }
                                }
                                else
                                {
                                    throw ex;
                                }
                            }
                        }
                        else
                        {
                            throw ex;
                        }
                    }
                }
                catch (Throwable ex)
                {
                    // Attempt to get the property with a set in front
                    try
                    {
                        try
                        {
                            loMethod = loObjectClass.getMethod(lcName, new Class[]{loValueClass});
                        }
                        catch (Exception e)
                        {
                            if (loPrimitive != null)
                            {
                                loMethod = loObjectClass.getMethod(lcName, new Class[]{loPrimitive});
                            }
                            else
                            {
                                throw ex;
                            }
                        }

                    }
                    catch (Exception innerEx)
                    {
                        Application.getInstance().log("The property " + lcName + " was not found on the object " + loObjectClass.getName(), LogType.WARNING());
                        // Property does not exist
                        continue;
                    }

                }

                loJDBCMethod.m_oMethod = loMethod;
                m_oMethods.put(i, loJDBCMethod);
            }
        }
        catch(Throwable ex)
        {
            Application.getInstance().log(ex);
        }
    }
    
    protected T onExecuteQuery(T toObject, JDBCQuery toQuery) throws Goliath.Exceptions.DataException
    {
        return onExecuteQuery(toObject, toQuery, new java.lang.Object[]{});
    }
    
    private T onExecuteQuery(T toObject, JDBCQuery toQuery, Object[] toParameter) throws Goliath.Exceptions.DataException
    {
        IConnection loConnection = null;
        try
        {
            loConnection = getConnection(toObject);
            loConnection.setAutoCommit(false);
            
            T loReturn = toQuery.query(toObject, loConnection, toParameter);

            loConnection.commit();

            return loReturn;
        }
        catch (Throwable e)
        {
            throw new Goliath.Exceptions.DataException(e);
        }
        finally
        {
            if (loConnection != null)
            {
                try
                {
                    loConnection.close();
                }
                catch (SQLException ex)
                {}
            }
        }
    }
    
    @Override
    protected T onGetObjectByKey(T toParameters, String tcKey) throws Goliath.Exceptions.DataException
    {
        return onExecuteQuery(toParameters, RETRIEVE(), new Object[]{tcKey});
    }
    
    @Override
    protected T onInsert(T toObject) throws Goliath.Exceptions.DataException
    {
        return onExecuteQuery(toObject, INSERT());
    }
    
    @Override
    protected T onDelete(T toObject) throws Goliath.Exceptions.DataException
    {
        return onExecuteQuery(toObject, DELETE());
    }
    
    @Override
    protected T onUpdate(T toObject) throws Goliath.Exceptions.DataException
    {
        return onExecuteQuery(toObject, UPDATE());
    }
    
    private Object[] getListResults(T toObject, String tcSelect, String tcCount) throws DataException
    {
        Object[] loCachedResults = null;
        int lnCount = 0;
        IConnection loConnection = getConnection(toObject);
        ResultSet loResults = null;
        try
        {
            Statement loState = loConnection.createStatement();
            loResults = loState.executeQuery(tcCount);
            if (loResults.next())
            {
                lnCount = loResults.getInt("Count");
                if (lnCount == 0)
                {
                    return null;
                }
            }
            
            // TODO: Implement hook for m_nResultCount too large to get result set
            CachedRowSetImpl loCachedResult = new CachedRowSetImpl();
            loCachedResult.setCommand(tcSelect);
            loCachedResult.execute(loConnection);
            m_oMetaData = loCachedResult.getMetaData();
            

            
            java.sql.PreparedStatement loStatement = loConnection.prepareStatement(tcSelect);
            loResults = loStatement.executeQuery();
            
            loCachedResults = createCachedResults(lnCount, loResults);
             

            return loCachedResult.toCollection().toArray();
        }
        catch (Throwable e)
        {
            Application.getInstance().log(tcCount);
            Application.getInstance().log(tcSelect);
            throw new DataException(e);
        }
        finally
        {
            if (loConnection != null)
            {
                try
                {
                    loResults.close();
                }
                catch (Throwable ex)
                {}
                try
                {
                    loConnection.close();
                }
                catch (Throwable ex)
                {}
            }
        }
    }

    private Object[] createCachedResults(int tnCount, ResultSet toResult)
    {
        try
        {
            CachedRowSetImpl loReturn = new CachedRowSetImpl();

            loReturn.setPageSize(tnCount);
            loReturn.populate(toResult);
            m_oMetaData = loReturn.getMetaData();

            return loReturn.toCollection().toArray();
        }
        catch (Throwable ex)
        {
            return null;
        }
    }

    @Override
    protected Object[] onGetList(T toParameters, String[] taWhereFields, String[] taOrderFields, boolean tlThrowError) throws DataException
    {
        String lcSelect = getGenerator(toParameters).generateSelectFromDataObject(toParameters, taWhereFields, taOrderFields);
        String lcSelectCount = getGenerator(toParameters).generateSelectCountFromDataObject(toParameters, taWhereFields, taOrderFields);
        return getListResults(toParameters, lcSelect, lcSelectCount);
    }
    
    @Override
    protected Object[] onGetListIn(T toParameters, ISimpleDataObjectCollection<?> toSearchCollection, String[] taInFields, String[] taInDataFields, String[] taOrderFields, boolean tlThrowError) throws DataException
    {
        T loObject = toParameters;
        String lcSelect = getGenerator(loObject).generateSelectFromDataObject(loObject, toSearchCollection, taInFields, taInDataFields, taOrderFields);
        String lcSelectCount = getGenerator(loObject).generateSelectCountFromDataObject(loObject, toSearchCollection, taInFields, taInDataFields, taOrderFields);
        return getListResults(loObject, lcSelect, lcSelectCount);
    }

    @Override
    protected T onConvertObject(T toParameter, Object toSource)
    {
        
        IDataMapper loMapper = DataMapper.getDataMapper(toParameter);

        try
        {
            com.sun.rowset.internal.Row loRow = (com.sun.rowset.internal.Row)toSource;

            T loObject = (T)toParameter.getClass().newInstance();

            loMapper.createFromDataSource(loObject, loRow);
            return loObject;
        }
        catch (Throwable ex)
        {
            Application.getInstance().log(ex);
        }
         

        // Could not create
        return null;
    }
     * 
     */

}
