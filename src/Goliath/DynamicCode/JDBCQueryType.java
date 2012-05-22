/* =========================================================
 * JDBCQuery.java
 *
 * Author:      kenmchugh
 * Created:     Sep 12, 2010, 1:57:15 PM
 *
 * Description
 * --------------------------------------------------------
 * <Description>
 *
 * Change Log
 * --------------------------------------------------------
 * Init.Date        Ref.            Description
 * --------------------------------------------------------
 *
 * =======================================================*/

package Goliath.DynamicCode;

import Goliath.Applications.Application;
import Goliath.Collections.HashTable;
import Goliath.Collections.List;
import Goliath.Data.Query.DataQuery;
import Goliath.Interfaces.Data.IConnection;
import Goliath.Interfaces.Data.ISimpleDataObject;
import Goliath.Interfaces.DynamicCode.ISqlGenerator;
import com.sun.rowset.CachedRowSetImpl;
import com.sun.rowset.internal.Row;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author kenmchugh
 */
public abstract class JDBCQueryType extends SQLQueryType
{
    private static HashTable<Class, HashTable<Integer, Method>> g_oMethods;
    private static HashTable<Class, ResultSetMetaData> g_oMetaData;
    
    
    private static JDBCQueryType g_oDelete;
    public static JDBCQueryType DELETE()
    {
        if (g_oDelete == null)
        {
            g_oDelete = createEnumeration(JDBCDeleteQuery.class, "JDBCDELETE");    
        }
        return g_oDelete;
    }
    
    private static JDBCQueryType g_oUpdate;
    public static JDBCQueryType UPDATE()
    {
        if (g_oUpdate == null)
        {
            g_oUpdate = createEnumeration(JDBCUpdateQuery.class, "JDBCUPDATE");    
        }
        return g_oUpdate;
    }
    
    private static JDBCQueryType g_oRetrieve;
    public static JDBCQueryType RETRIEVE()
    {
        if (g_oRetrieve == null)
        {
            g_oRetrieve = createEnumeration(JDBCRetrieveQuery.class, "JDBCRETRIEVE");    
        }
        return g_oRetrieve;
    }
    
    private static JDBCQueryType g_oInsert;
    public static JDBCQueryType INSERT()
    {
        if (g_oInsert == null)
        {
            g_oInsert = createEnumeration(JDBCInsertQuery.class, "JDBCINSERT");    
        }
        return g_oInsert;
    }
    
    /**
     * Sets all of the metadata for the specified class
     * @param toClass the class to set the metadata for
     * @param toMetaData the metadata to set
     */
    protected static void setMetaData(Class toClass, ResultSetMetaData toMetaData)
    {
        if (g_oMetaData == null)
        {
            g_oMetaData = new HashTable<Class, ResultSetMetaData>();
        }
        if (!g_oMetaData.containsKey(toClass))
        {
            g_oMetaData.put(toClass, toMetaData);
        }
    }

    /**
     * Gets the metadata for the specified class
     * @param toClass the class to get the metadata for
     * @return the meta data
     */
    private static ResultSetMetaData getMetaData(Class toClass)
    {
        return g_oMetaData != null ? g_oMetaData.get(toClass) : null;
    }
    
    /**
     * Gets the list of methods for the class specified and links them to a field in the data store
     * @param toConnection the connection to create or get the the links for
     * @param toClass the class to get the links for
     * @return the list if methods keyed by field in the data store
     */
    private static HashTable<Integer, Method> getMethods(IConnection toConnection, Class toClass)
    {
        if (g_oMethods == null)
        {
            g_oMethods = new HashTable<Class, HashTable<Integer, Method>>();
        }
        if (!g_oMethods.contains(toClass))
        {
            g_oMethods.put(toClass, new HashTable<Integer, Method>());

            ResultSetMetaData loMetaData = getMetaData(toClass);
            if (loMetaData != null)
            {
                ISqlGenerator loGenerator = getGenerator(toConnection);
                try
                {
                    for (int i=0, lnLength = loMetaData.getColumnCount(); i<lnLength; i++)
                    {
                        String lcName = loMetaData.getColumnName(i+1);
                        if (lcName.equalsIgnoreCase(loGenerator.getKeyName(toClass)))
                        {
                            lcName = "ID";
                        }
                        g_oMethods.get(toClass).put(i+1, Java.getMutatorMethod(toClass, lcName, false));
                    }
                }
                catch(Throwable ex)
                {
                    Application.getInstance().log(ex);
                }
            }
        }
        return g_oMethods.get(toClass);
    }
    
    /**
     * Populates the data object from the raw form, using the connection to get the method map
     * @param toConnection the connection to use for retrieveing the method map
     * @param toObject the data object to populate
     * @param toSource the raw data
     */
    public static void populateObject(IConnection toConnection, ISimpleDataObject toObject, Object toSource)
    {
        Row loRow = (Row)toSource;
        HashTable<Integer, Method> loMethods = getMethods(toConnection, toObject.getClass());

        if (loMethods != null)
        {
            try
            {
                for (Integer loKey : loMethods.keySet())
                {
                    ISqlGenerator loGenerator = toConnection.getQueryGenerator();
                    loMethods.get(loKey).invoke(toObject, new java.lang.Object[]{loGenerator.formatFromSQL(loRow.getColumnObject(loKey), loMethods.get(loKey).getParameterTypes()[0])});
                }
            }
            catch (Throwable e)
            {
                throw new Goliath.Exceptions.DataException(e);
            }
        }
    }
    
    /**
     * Gets the Query generator for the connection specified
     * @param toConnection the connection to get the generator for
     * @return the query generator
     */
    public static ISqlGenerator getGenerator(IConnection toConnection)
    {
        return toConnection.getQueryGenerator();
    }
    
    
    
    /**
     * Creates a new instance of the query type
     * @param tcValue 
     */
    protected JDBCQueryType(String tcValue)
    {
        super(tcValue);
    }
    
    /**
     * Creates the statement for the connection
     * @param toConnection the connection to create the statement for
     * @param tcQuery the query to run
     * @return the Prepared Statement
     * @throws SQLException if there are issues with the query syntax
     */
    protected PreparedStatement prepareStatement(IConnection toConnection, String tcQuery) throws SQLException
    {
        return toConnection.prepareStatement(tcQuery, Statement.RETURN_GENERATED_KEYS);
    }
    
    /**
     * Queries all of the data objects from the data source that match the type and are successfully 
     * filtered by the query arguments, if the query arguments are null then all of the items will
     * be returned
     * @param toObject the object type to get
     * @param toConnection the connection to use to retrieve the items
     * @param toArgs the query arguments
     * @param toProperties list of values to be used as needed
     * @return the result of the query
     */
    public final List query(ISimpleDataObject toObject, IConnection toConnection, DataQuery toArgs, List<Object> toProperties)
    {
        String lcQuery = generateCode(toConnection, toObject, toArgs, toProperties);
        
        java.sql.PreparedStatement loStatement = null;
        try
        {
            loStatement = prepareStatement(toConnection, lcQuery);

            // Add the byte stream if required
            if (lcQuery.indexOf("?") >= 0)
            {
                int lnCount = 1;
                Java.ClassDefinition loDef = Java.getClassDefinition(toObject.getClass());
                for (String lcProperty : loDef.getProperties())
                {
                    Object loValue = Goliath.DynamicCode.Java.getPropertyValue(toObject, lcProperty);
                    // TODO : Make this specific to byte arrays
                    if (loValue != null && loValue.getClass().isArray())
                    {
                        ByteArrayInputStream loStream = new ByteArrayInputStream((byte[])loValue);
                        loStatement.setBinaryStream(lnCount, loStream, ((byte[])loValue).length);
                        lnCount++;
                    }
                }
            }

            return onQuery(toObject, loStatement);

        }
        catch (Throwable ex)
        {
            Application.getInstance().log(lcQuery);
            Application.getInstance().log(ex);
        }
        finally
        {
            if (loStatement != null)
            {
                try
                {
                    loStatement.close();
                }
                catch (Throwable ignore)
                {}
            }
        }
        return null;
    }
    
    /**
     * Method that does the actual query to the data source
     * @param toObject the object that is being used for the query
     * @param toStatement the prepared statement that is being executed
     * @return the results of the query in raw form
     * @throws Throwable if any errors occur
     */
    protected abstract List onQuery(ISimpleDataObject toObject, java.sql.PreparedStatement toStatement) throws Throwable;
    
    
    
    /**
     * This class generates the delete query and gets the results of a deletion
     */
    public static class JDBCDeleteQuery extends JDBCQueryType
    {
        protected JDBCDeleteQuery(String tcValue)
        {
            super(tcValue);
        }

        @Override
        protected List onQuery(ISimpleDataObject toObject, java.sql.PreparedStatement toStatement) throws Throwable
        {
            toStatement.execute();

            return null;
        }

        @Override
        public String generateCode(IConnection toConnection, ISimpleDataObject toObject, DataQuery toArguments, List<Object> toProperties)
        {
            return getGenerator(toConnection).generateDeleteFromDataObject(toObject, toArguments);
        }
    }
    
    public static class JDBCUpdateQuery extends JDBCQueryType
    {
        protected JDBCUpdateQuery(String tcValue)
        {
            super(tcValue);
        }
        
        @Override
        public String generateCode(IConnection toConnection, ISimpleDataObject toObject, DataQuery toArguments, List<Object> toProperties)
        {
            return getGenerator(toConnection).generateUpdateFromDataObject(toObject, toArguments, toProperties);
        }
        
        @Override
        protected List onQuery(ISimpleDataObject toObject, java.sql.PreparedStatement toStatement) throws Throwable
        {
            java.sql.ResultSet loResults = null;
            toStatement.execute();
            try
            {
                loResults = toStatement.getGeneratedKeys();
                if (loResults != null && loResults.next())
                {
                    toObject.setID(loResults.getBigDecimal(1).longValue());
                    // TODO : Need to also autoupdate the rowversion and modified by, modified date
                }
                return new List<ISimpleDataObject>(new ISimpleDataObject[]{toObject});
            }
            finally
            {
                if (loResults != null)
                {
                    try
                    {
                        loResults.close();
                    }
                    catch (Throwable ignore)
                    {}
                }
            }
        }
    }
    
    public static class JDBCInsertQuery extends JDBCQueryType
    {
        protected JDBCInsertQuery(String tcValue)
        {
            super(tcValue);
        }
        
        @Override
        public String generateCode(IConnection toConnection, ISimpleDataObject toObject, DataQuery toArguments, List<Object> toProperties)
        {
            return getGenerator(toConnection).generateInsertFromDataObject(toObject, toArguments);
        }
        
        @Override
        protected List onQuery(ISimpleDataObject toObject, java.sql.PreparedStatement toStatement) throws Throwable
        {
            java.sql.ResultSet loResults = null;
            toStatement.execute();
            try
            {

                loResults = toStatement.getGeneratedKeys();
                if (loResults != null && loResults.next())
                {
                    toObject.setID(loResults.getBigDecimal(1).longValue());
                    // TODO : Need to also autoupdate the rowversion and modified by, modified date
                }
                return new List<ISimpleDataObject>(new ISimpleDataObject[]{toObject});
            }
            finally
            {
                loResults.close();
            }
        }

        @Override
        public PreparedStatement prepareStatement(IConnection toConnection, String tcQuery) throws SQLException
        {
            return toConnection.prepareStatement(tcQuery,Statement.RETURN_GENERATED_KEYS);
        }
    }

    public static class JDBCRetrieveQuery extends JDBCQueryType
    {
        protected JDBCRetrieveQuery(String tcValue)
        {
            super(tcValue);
        }
        
        @Override
        public String generateCode(IConnection toConnection, ISimpleDataObject toObject, DataQuery toArguments, List<Object> toProperties)
        {
            // Generate a retrieve query
            return getGenerator(toConnection).generateSelectFromDataObject(toObject, toArguments);
        }

        private List createCachedResults(int tnCount, ResultSet toResult, Class toClass)
        {
            try
            {
                CachedRowSetImpl loReturn = new CachedRowSetImpl();
                
                loReturn.setPageSize(tnCount);
                loReturn.populate(toResult);
                setMetaData(toClass, loReturn.getMetaData());

                return new List(loReturn.toCollection());
            }
            catch (Throwable ex)
            {
                return null;
            }
        }

        @Override
        protected List onQuery(ISimpleDataObject toObject, java.sql.PreparedStatement toStatement) throws Throwable
        {
            java.sql.ResultSet loResults = null;
            try
            {
                loResults = toStatement.executeQuery();

                // TODO: Need to programmatically adjust this value for speed
                return createCachedResults(1, loResults, toObject.getClass());
            }
            catch (Throwable ex)
            {
                Application.getInstance().log(toStatement.toString());
                Application.getInstance().log(ex);
                return null;
            }
            finally
            {
                loResults.close();
            }
        }
    }
}
