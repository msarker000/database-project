package edu.ccny.db.project;

public class Column implements Cloneable {

	private final Character name;
	private final Datatype type;
	private String value;

	public Column(Character name, Datatype type) {
		this.name = name;
		this.type = type;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	public Character getName() {
		return name;
	}

	public Datatype getType() {
		return type;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Column other = (Column) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (type != other.type)
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Column [name=" + name + ", type=" + type + ", value=" + value + "]";
	}
	

	public String printableFormat() {
		return "name: "+name + ", dataType: " + type ;
	}

	@Override
	protected Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

	public static void main(String[] args) throws CloneNotSupportedException {
		Column column = new Column('A', Datatype.STRING);
		column.setValue("ayub");

		Column column2 = (Column) column.clone();
		column2.setValue("Yakub");

		System.out.println(column);
		System.out.println(column2);
	}

}
