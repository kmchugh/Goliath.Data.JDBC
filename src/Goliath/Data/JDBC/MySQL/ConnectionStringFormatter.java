/* =========================================================
 * JDBCMySQLStringFormatter.java
 *
 * Author:      kmchugh
 * Created:     14-Dec-2007, 18:16:57
 * 
 * Description
 * --------------------------------------------------------
 * Formats the connection string for a MySQL JDBC connection
 *
 * Change Log
 * --------------------------------------------------------
 * Init.Date        Ref.            Description
 * --------------------------------------------------------
 * 
 * =======================================================*/

package Goliath.Data.JDBC.MySQL;

import Goliath.Collections.List;
import Goliath.Constants.StringFormatType;
import Goliath.Interfaces.Data.IConnectionString;
import Goliath.Text.StringFormatter;

/**
 * Formats the connection string for a MySQL JDBC connection
 *
 * @see         Goliath.Data.ConnectionString
 * @version     1.0 14-Dec-2007
 * @author      kmchugh
**/
public class ConnectionStringFormatter extends Goliath.Text.StringFormatter<Goliath.Interfaces.Data.IConnectionString>
{
    /** Creates a new instance of JDBCMySQLStringFormatter */
    public ConnectionStringFormatter()
    {
    }

    @Override
    public String toString(Goliath.Interfaces.Data.IConnectionString toObject)
    {
        StringBuilder loBuilder = new StringBuilder("jdbc:mysql://");
        String lcPort = toObject.<String>getParameter("port");
        String lcDatabase = toObject.<String>getParameter("database");
        
        loBuilder.append(toObject.<String>getParameter("hostname"));
        
        if (lcPort != null && !lcPort.isEmpty())
        {
            loBuilder.append(":" + lcPort);
        }
        
        loBuilder.append("/");
        
        if (lcDatabase != null && !lcDatabase.isEmpty())
        {
            loBuilder.append( lcDatabase);
        }
        
        loBuilder.append("?");
        
        boolean llAmp = false;
        for (String lcParameterName : toObject.getParameters().getPropertyKeys())
        {
            if (!lcParameterName.equalsIgnoreCase("port") &&
                    !lcParameterName.equalsIgnoreCase("database") &&
                    !lcParameterName.equalsIgnoreCase("hostname") &&
                    toObject.getParameters().getProperty(lcParameterName) != null)
            {
                loBuilder.append((llAmp ? "&" : "") + lcParameterName + "=" + toObject.getParameters().getProperty(lcParameterName).toString());
                llAmp = true;
            }
        }
        
        return loBuilder.toString();
    }
    
    @Override
    public void appendPrimitiveString(StringBuilder toBuilder, IConnectionString toObject, StringFormatType toType)
    {
        
    }

    @Override
    protected void formatComplexProperty(StringBuilder toBuilder, String tcPropertyName, Object toValue, StringFormatter toFormatter, StringFormatType toType)
    {
        
    }

    @Override
    protected void formatForPropertyCount(StringBuilder toBuilder, int tnIndex, int tnCount, StringFormatType toFormatType)
    {
        
    }

    @Override
    public String formatNullObject()
    {
        return "";
    }

    @Override
    protected String getEndTag(IConnectionString toObject)
    {
        return "";
    }

    @Override
    protected String getStartTag(IConnectionString toObject)
    {
        return "";
    }

    @Override
    public List<StringFormatType> supportedFormats()
    {
        return new List<StringFormatType>(new StringFormatType[]{StringFormatType.DEFAULT()});
    }
    
    
}
