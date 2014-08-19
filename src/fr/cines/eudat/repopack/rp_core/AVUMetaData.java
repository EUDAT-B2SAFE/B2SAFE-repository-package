package fr.cines.eudat.repopack.rp_core;

public class AVUMetaData {
	
	// TODO handle some constants to deal with metadata name
	
	private String attribute=null;
	private String value=null;
	private String unit=null;
	
	public AVUMetaData(String attribute, String value){
		this.attribute = attribute;
		this.value = value;
		this.unit = null;
	}
	
	public AVUMetaData(String attribute, String value, String unit){
		this.attribute = attribute;
		this.value = value;
		this.unit = unit;
	}
	
	public String getAttribute() {
		return this.attribute;
	}
	
	public String getValue() {
		return this.value;
	}
	
	public String getUnit() {
		return this.value;
	}
	
	@Override
    public String toString()
    {
        StringBuilder sb= new StringBuilder();
        sb.append("[AVU => ");
        sb.append(attribute);
        sb.append("=");
        sb.append(value);
        if (unit!=null){ sb.append("**"); sb.append(unit);}
        sb.append(" ]");
        return sb.toString();

    }


}
