package pdi.bridge;

import java.io.File;
import java.io.FilenameFilter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedList;
import java.util.List;

public class KettleBridge {

  private static final String NO_KETTLE = "Couldn't find KETTLE_HOME";

  private static String KETTLE_HOME;

  private static ClassLoader KETTLE_CLASS_LOADER = null;

  private static Class<?> transClass = null;
  private static Class<?> transMetaClass = null;
  private static Class<?> transRunnerClass = null;
  private static Class<?> fieldHolderClass = null;

  private static boolean isInitialized = false;

  public static ClassLoader getKettleClassloader() throws Exception {
    if ( KETTLE_CLASS_LOADER != null ) {
      return KETTLE_CLASS_LOADER;
    }

    KETTLE_HOME = System.getProperty( "KETTLE_HOME", null );
    if ( KETTLE_HOME == null ) {
      KETTLE_HOME = System.getenv( "KETTLE_HOME" );
      if ( KETTLE_HOME == null || KETTLE_HOME.isEmpty() ) {
        throw new Exception( NO_KETTLE );
      }
    }

    if ( !KETTLE_HOME.endsWith( File.separator ) ) {
      KETTLE_HOME += File.separator;
    }

    String KETTLE_LIB = KETTLE_HOME + "lib";

    final File KETTLE_LIB_DIR = new File( KETTLE_LIB );
    if ( !KETTLE_LIB_DIR.exists() ) {
      throw new Exception( NO_KETTLE );
    }

    List<URL> urls = new LinkedList<URL>();
    for ( File jarFile : KETTLE_LIB_DIR.listFiles(
      new FilenameFilter() {
        @Override
        public boolean accept( File dir, String name ) {
          return ( name.endsWith( ".jar" ) );
        }
      } )
      ) {
      urls.add( jarFile.toURI().toURL() );
    }

    // Get co-located pdi-runner JAR
    File myDir = new File( KettleBridge.class.getProtectionDomain().getCodeSource().getLocation().toURI() );
    for ( File jarFile : myDir.getParentFile().listFiles(
      new FilenameFilter() {
        @Override
        public boolean accept( File dir, String name ) {
          return ( name.startsWith( "pdi-bridge" ) && name.endsWith( ".jar" ) );
        }
      } )
      ) {
      urls.add( jarFile.toURI().toURL() );
    }

    KETTLE_CLASS_LOADER =
      new URLClassLoader( urls.toArray( new URL[]{ } ), KettleBridge.class.getClassLoader() );
    return KETTLE_CLASS_LOADER;
  }

  public static Object startTransformation( String filename, List<Object> fields ) throws Exception {
    Thread.currentThread().setContextClassLoader( getKettleClassloader() );
    Constructor<?> transMetaCtor = transMetaClass.getConstructor( String.class );
    Object transMeta = transMetaCtor.newInstance( filename );
    Constructor<?> transCtor = transClass.getConstructor( transMetaClass );
    Object trans = transCtor.newInstance( transMeta );
    Object transRunner = transRunnerClass.newInstance();

    Method start = transRunnerClass.getMethod( "start", transClass, List.class );
    start.invoke( transRunner, trans, fields );
    return transRunner;
  }

  public static void finishTransformation( Object transRunner ) throws Exception {
    Thread.currentThread().setContextClassLoader( getKettleClassloader() );

    Method finish = transRunnerClass.getMethod( "finish" );
    finish.invoke( transRunner );
  }

  public static void addRow( Object transRunner, List<Object> values ) throws Exception {
    Thread.currentThread().setContextClassLoader( getKettleClassloader() );
    Method addRow = transRunnerClass.getMethod( "addRow", List.class );
    addRow.invoke( transRunner, values );
  }

  public static Object[] nextRow( Object transRunner ) throws Exception {
    Method nextRow = transRunnerClass.getMethod( "nextRow" );
    return (Object[]) nextRow.invoke( transRunner );
  }

  public static Object getFieldHolder( String name, String type ) throws Exception {
    Constructor<?> fieldHolderCtor = fieldHolderClass.getConstructor( String.class, String.class );
    return fieldHolderCtor.newInstance( name, type );
  }

  public static void init() throws Exception {
    if ( isInitialized ) {
      return;
    }
    if ( System.getProperty( "KETTLE_PLUGIN_BASE_FOLDERS", null ) == null ) {
      String pluginFolder = new File( KETTLE_HOME + "plugins" ).toURI().toString();
      System.setProperty( "KETTLE_PLUGIN_BASE_FOLDERS", pluginFolder );
    }
    Thread.currentThread().setContextClassLoader( getKettleClassloader() );
    Class<?> kettleEnvironment = getKettleClassloader().loadClass( "org.pentaho.di.core.KettleEnvironment" );
    Method isInit = kettleEnvironment.getMethod( "isInitialized" );
    boolean initialized = (Boolean) isInit.invoke( null );

    if ( !initialized ) {
      Method init = kettleEnvironment.getMethod( "init", Boolean.TYPE );
      init.invoke( null, false );
      isInitialized = true;
    }

    transClass = getKettleClassloader().loadClass( "org.pentaho.di.trans.Trans" );
    transMetaClass = getKettleClassloader().loadClass( "org.pentaho.di.trans.TransMeta" );
    transRunnerClass = getKettleClassloader().loadClass( "pdi.runner.TransRunner" );
    fieldHolderClass = getKettleClassloader().loadClass( "pdi.runner.FieldHolder" );

  }
}
