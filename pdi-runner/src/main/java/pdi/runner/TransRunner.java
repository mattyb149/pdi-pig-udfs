package pdi.runner;

import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.step.RowAdapter;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by mburgess on 12/10/14.
 */
public class TransRunner {

  private RowProducer rowProducer;
  private RowMetaInterface rowMeta;
  private Trans trans;
  private BlockingQueue<Object[]> q;

  public void start( Trans t, List<FieldHolder> fields ) throws Exception {
    trans = t;
    q = new LinkedBlockingQueue<Object[]>();

    trans.prepareExecution( null );
    trans.getStepInterface( "OUTPUT", 0 ).addRowListener( new OutputRowCollector( q ) );
    rowProducer = trans.addRowProducer( "INPUT", 0 );

    trans.startThreads();

    // add rows
    rowMeta = new RowMeta();
    for ( FieldHolder field : fields ) {
      rowMeta.addValueMeta( new ValueMeta( field.getName(), ValueMetaFactory.getIdForValueMeta( field.getType() ) ) );
    }
  }

  public void addRow( List<Object> values ) {

    rowProducer.putRow( rowMeta, values.toArray() );
  }

  public Object[] nextRow() throws Exception {
    return q.poll( 3, TimeUnit.SECONDS );
  }

  public void finish() {
    rowProducer.finished();
    trans.waitUntilFinished();
  }

  public static class OutputRowCollector extends RowAdapter {
    Queue<Object[]> q;

    public OutputRowCollector( Queue<Object[]> q ) {
      this.q = q;
    }

    @Override
    public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
      q.add( row );
    }
  }

}
