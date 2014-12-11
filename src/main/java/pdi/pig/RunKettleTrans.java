package pdi.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import pdi.bridge.KettleBridge;

public class RunKettleTrans extends EvalFunc<Tuple> {

  private static final ClassLoader kettleClassLoader;
  private Object transRunner = null;

  static {
    try {
      kettleClassLoader = KettleBridge.getKettleClassloader();
    } catch ( Exception e ) {
      throw new RuntimeException( e );
    }
  }

  public Tuple exec( Tuple input ) throws IOException {
    try {
      KettleBridge.init();
      if ( input == null || input.size() == 0 || input.get( 0 ) == null ) {
        return null;
      }

      // First arg is the transRunner filename
      if ( transRunner == null ) {
        String filename = (String) input.get( 0 );
        Schema inputSchema = getInputSchema();
        inputSchema.getFields().remove( 0 );
        List<Object> fields = getFieldHolderList( inputSchema );
        transRunner = KettleBridge.startTransformation( filename, fields );
      }

      TupleFactory tf = TupleFactory.getInstance();
      Tuple outTuple = tf.newTuple();
      // Get incoming row, remove the transformation name, then inject it into the trans
      List<Object> inRow = input.getAll();
      inRow.remove( 0 );
      KettleBridge.addRow( transRunner, inRow );
      Object[] row = KettleBridge.nextRow( transRunner );
      if ( row != null ) {
        for ( Object val : row ) {
          if ( val != null ) {
            outTuple.append( val );
          }
        }
        return outTuple;
      } else {
        return null;
      }

    } catch ( Exception e ) {
      throw new IOException( "Caught exception processing input row ", e );
    }
  }

  public Schema outputSchema( Schema input ) {
    return super.outputSchema( input );
    /* TODO
    try {
      Schema s = new Schema();

      s.add(new Schema.FieldSchema("action", DataType.CHARARRAY));
      s.add(new Schema.FieldSchema("ip", DataType.CHARARRAY));
      s.add(new Schema.FieldSchema("date", DataType.CHARARRAY));

      return s;
    } catch (Exception e) {
      // Any problems? Just return null...there probably won't be any
      // problems though.
      return null;
    }*/
  }

  /**
   * Placeholder for cleanup to be performed at the end. User defined functions can override.
   * Default implementation is a no-op.
   */
  @Override
  public void finish() {
    try {
      KettleBridge.finishTransformation( transRunner );
    } catch ( Exception e ) {
      log.error( e );
    }
  }

  public static List<Object> getFieldHolderList( Schema s ) throws Exception {
    List<FieldSchema> fields = s.getFields();
    List<Object> fieldHolderList = new ArrayList<Object>( fields.size() );
    for ( FieldSchema field : fields ) {
      fieldHolderList.add( KettleBridge.getFieldHolder( field.alias, getKettleType( field.type ) ) );
    }
    return fieldHolderList;
  }

  private static String getKettleType( byte type ) {

    switch ( type ) {
      case DataType.BYTE:
      case DataType.BYTEARRAY:
        return "Binary";
      case DataType.BOOLEAN:
        return "Boolean";
      case DataType.CHARARRAY:
      case DataType.BIGCHARARRAY:
        return "String";
      case DataType.DATETIME:
        return "Timestamp";
      case DataType.INTEGER:
      case DataType.LONG:
        return "Integer";
      case DataType.BIGINTEGER:
      case DataType.BIGDECIMAL:
        return "BigNumber";
      case DataType.FLOAT:
      case DataType.DOUBLE:
        return "Number";
      default:
        return "Binary"; // TODO is this right?
    }
  }

}
