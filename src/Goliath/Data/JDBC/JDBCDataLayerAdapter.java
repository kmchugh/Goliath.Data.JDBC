/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package Goliath.Data.JDBC;

import Goliath.Applications.Application;
import Goliath.Collections.List;
import Goliath.Constants.LogType;
import Goliath.Data.Query.DataQuery;
import Goliath.Data.Query.InList;
import Goliath.DynamicCode.JDBCQueryType;
import Goliath.Interfaces.Data.IConnection;
import Goliath.Interfaces.Data.IConnectionString;
import Goliath.Interfaces.Data.IDataBase;
import Goliath.Interfaces.Data.ISimpleDataObject;
import Goliath.Interfaces.Data.ITable;
import Goliath.Interfaces.DynamicCode.ISqlGenerator;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;

/**
 *
 * @author kmchugh
 */
public abstract class JDBCDataLayerAdapter extends Goliath.Data.DataAdapters.DataLayerAdapter
{
    @Override
    protected <T extends ISimpleDataObject> T onGetByGUID(Class<T> toClass, String tcGUID)
    {
        List loItems = onGetRawList(toClass, new InList<String, String>("GUID", new String[]{tcGUID}), 1);
        return loItems.size() == 1 ? onConvertObject(toClass, loItems.get(0)) : null;
    }
    
    @Override
    protected <T extends ISimpleDataObject> T onGetByID(Class<T> toClass, long tnID)
    {
        List loItems = onGetRawList(toClass, new InList<Long, Long>("ID", new Long[]{tnID}), 1);
        return loItems.size() == 1 ? onConvertObject(toClass, loItems.get(0)) : null;
    }
    
    @Override
    public boolean onCreateDataObject(ISimpleDataObject toDataObject)
    {
        JDBCQueryType loQuery = JDBCQueryType.INSERT();
        IConnection loConnection = getConnection();

        if (loConnection != null)
        {
            try
            {
                loConnection.setAutoCommit(false);

                loQuery.query(toDataObject, loConnection, null, null);

                loConnection.commit();

                Application.getInstance().log("Created DataObject " + toDataObject.getClass().getSimpleName() + " " + 
                        (toDataObject.hasGUID() ? toDataObject.getGUID() : toDataObject.getID()), LogType.TRACE());
                return true;
            }
            catch (SQLSyntaxErrorException ex)
            {
                Application.getInstance().log(ex);
            }
            catch (Exception e)
            {
                Application.getInstance().log(e);
            }
            finally
            {
                if (loConnection != null)
                {
                    try
                    {
                        loConnection.close();
                    }
                    catch (Throwable ex)
                    {}
                }
            }
        }
        return false;
    }

    @Override
    public boolean onUpdateDataObject(ISimpleDataObject toDataObject, List<String> toProperties, DataQuery toQuery)
    {
        JDBCQueryType loQuery = JDBCQueryType.UPDATE();
        IConnection loConnection = getConnection();

        if (loConnection != null)
        {
            try
            {
                loConnection.setAutoCommit(false);

                loQuery.query(toDataObject, loConnection, toQuery, new List<Object>(toProperties));

                loConnection.commit();

                Application.getInstance().log("Updated DataObject " + toDataObject.getClass().getSimpleName() + " " + toDataObject.getGUID(), LogType.TRACE());
                return true;
            }
            catch (SQLSyntaxErrorException ex)
            {
                Application.getInstance().log(ex);
            }
            catch (Exception e)
            {
                Application.getInstance().log(e);
            }
            finally
            {
                if (loConnection != null)
                {
                    try
                    {
                        loConnection.close();
                    }
                    catch (Throwable ex)
                    {}
                }
            }
        }
        return false;
    }

    @Override
    protected boolean onDeleteDataObject(ISimpleDataObject toDataObject, DataQuery toQuery)
    {
        JDBCQueryType loQuery = JDBCQueryType.DELETE();
        IConnection loConnection = getConnection();

        if (loConnection != null)
        {
            try
            {
                loConnection.setAutoCommit(false);

                loQuery.query(toDataObject, loConnection, toQuery, null);

                loConnection.commit();

                Application.getInstance().log("Deleted DataObject " + toDataObject.getClass().getSimpleName() + " " + (toDataObject.hasGUID() ? toDataObject.getGUID() : toDataObject.getID()), LogType.TRACE());
                return true;
            }
            catch (SQLSyntaxErrorException ex)
            {
                Application.getInstance().log(ex);
            }
            catch (Exception e)
            {
                Application.getInstance().log(e);
            }
            finally
            {
                if (loConnection != null)
                {
                    try
                    {
                        loConnection.close();
                    }
                    catch (Throwable ex)
                    {}
                }
            }
        }
        return false;
    }
    
    
    
    @Override
    protected <T extends ISimpleDataObject> List onGetRawList(Class<T> toClass, DataQuery toQuery, long tnMaxItems)
    {
        JDBCQueryType loQuery = JDBCQueryType.RETRIEVE();
        IConnection loConnection = getConnection();

        if (loConnection != null)
        {
            try
            {
                loConnection.setAutoCommit(false);

                List loReturn = (List)loQuery.query(toClass.newInstance(), loConnection, toQuery, null);

                loConnection.commit();

                return loReturn;
            }
            catch (SQLSyntaxErrorException ex)
            {
                Application.getInstance().log(ex);
            }
            catch (Throwable e)
            {
                Application.getInstance().log(e);
            }
            finally
            {
                if (loConnection != null)
                {
                    try
                    {
                        loConnection.close();
                    }
                    catch (Throwable ex)
                    {}
                }
            }
        }
        return new List(0);
    }
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

    

    @Override
    protected <T extends ISimpleDataObject> T onConvertObject(Class<T> toClass, Object toObject)
    {
        try
        {
            T loObject = toClass.newInstance();
            populateFromDataSource(loObject, toObject);
            return loObject;
        }
        catch (Throwable ex)
        {
            Application.getInstance().log(ex);
        }


        // Could not create
        return null;
    }

    private <T extends ISimpleDataObject> void populateFromDataSource(T toObject, Object toSource)
    {
        // Create the Connection
        IConnection loConnection = getConnection();
        try
        {
            // We will ask the JDBCQuery to do that transformation
            JDBCQueryType.populateObject(loConnection, toObject, toSource);
        }
        catch (Throwable ex)
        {
            Application.getInstance().log(ex);
        }
        finally
        {
            if (loConnection != null)
            {
                try
                {
                    loConnection.close();
                }
                catch (Throwable ex)
                {
                    Application.getInstance().log(ex);
                }
            }
        }

    }

    /** Creates a new instance of MySQLConnectionType */
    public JDBCDataLayerAdapter(IConnectionString toConnectionString)
    {
        super(toConnectionString);
    }

    @Override
    protected void onCreateDataBaseFailed(IDataBase toDataBase)
    {
    }

    @Override
    protected void onInitialiseDataBaseFailed(IDataBase toDataBase)
    {
    }

    @Override
    protected boolean onAfterCreateDataBase(IDataBase toDataBase)
    {
        return true;
    }

    @Override
    protected boolean onBeforeCreateDataBase(IDataBase toDataBase)
    {
        return true;
    }

    @Override
    protected boolean onCreateUsers(IDataBase toDataBase)
    {
        return true;
    }

    @Override
    protected boolean onInitialiseDataBase(IDataBase toDataBase)
    {
        return true;
    }

    /**
     * Adds all the possible parameter to the connection type
     */
    @Override
    protected void onAddParameters()
    {
        // The user to connect as
        this.addParameter("user");
        // The password to use when connecting
        this.addParameter("password");
    }

    @Override
    protected boolean onAfterCreateTable(ITable toTable)
    {
        return true;
    }

    @Override
    protected boolean onBeforeCreateTable(ITable toTable)
    {
        return true;
    }

    @Override
    protected boolean onCreateTable(ITable toTable)
    {
        // TODO: Should be using the SQL generator
        IConnection loConnection = getConnection();
        if (loConnection != null)
        {
            Statement loStatement = null;
            String lcSelect = null;
            try
            {
                Goliath.Interfaces.DynamicCode.ISqlGenerator loGenerator = loConnection.getQueryGenerator();
                lcSelect = loGenerator.generateCreateTable(toTable);
                loStatement = loConnection.createStatement();

                loStatement.executeUpdate(lcSelect);
                return true;
            }
            catch (SQLSyntaxErrorException ex)
            {
                Application.getInstance().log(lcSelect, LogType.TRACE());
                Application.getInstance().log(ex);
            }
            catch (SQLException ex)
            {
                Application.getInstance().log(lcSelect, LogType.TRACE());
                Application.getInstance().log(ex);
            }
            catch (Exception e)
            {
                Application.getInstance().log(e);
            }
            finally
            {
                closeConnectionAndStatement(loConnection, loStatement);
            }
        }
        return false;
    }



    

    




    @Override
    protected boolean onTableExists(ITable toTable)
    {
        IConnection loConnection = getConnection();

        if (loConnection != null)
        {
            ISqlGenerator loGenerator = loConnection.getQueryGenerator();
            String lcStatement = loGenerator.generateTableExists(toTable);
            PreparedStatement loStatement = null;
            try
            {
                loStatement = loConnection.prepareStatement(lcStatement);
                loStatement.setMaxRows(1);

                java.sql.ResultSet loResults = loStatement.executeQuery();
                loResults.close();
                return true;
            }
            catch (SQLSyntaxErrorException ex)
            {
                if (ex.getErrorCode() != 1146 &&
                        ex.getErrorCode() != 30000) // Table doesn't exist error
                {
                    Application.getInstance().log(lcStatement, LogType.TRACE());
                    Application.getInstance().log(ex);
                }
            }
            catch (Exception e)
            {
            }
            finally
            {
                closeConnectionAndStatement(loConnection, loStatement);
            }
        }
        return false;
    }






    @Override
    protected void onCreateTableFailed(ITable toTable)
    {
    }



    /**
     * Helper function for safely closing the statement and connection
     * @param toConnection the connection to close
     * @param toStatement the statement to close
     */
    protected final void closeConnectionAndStatement(IConnection toConnection, Statement toStatement)
    {
        if (toConnection != null)
        {
            try
            {
                toConnection.close();
            }
            catch (Throwable ex)
            {}
        }
        if (toStatement != null)
        {
            try
            {
                toStatement.close();
            }
            catch (Throwable ex)
            {}
        }
    }
}