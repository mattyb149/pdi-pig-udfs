package pdi.runner;

/**
 * Created by mburgess on 12/10/14.
 */
public class FieldHolder {

  private String name;
  private String type;

  public FieldHolder( String name, String type ) {
    this.name = name;
    this.type = type;
  }

  public String getName() {

    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType( String type ) {
    this.type = type;
  }


}
