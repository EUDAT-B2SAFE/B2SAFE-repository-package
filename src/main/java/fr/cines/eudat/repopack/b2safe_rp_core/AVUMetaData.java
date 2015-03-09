package fr.cines.eudat.repopack.b2safe_rp_core;

/**
 * This class represents a metadata with an Attribute, Value, Unit format
 * Getter and setter methods are available to handle the different properties
 * 
 * @author "S. Coutin (CINES)"
 *
 */
public class AVUMetaData {
	
	// TODO handle some constants to deal with metadata name
	
	private String attribute=null;
	private String value=null;
	private String unit=null;
	
	/**
	 * Constructor with attribute and value only
	 * 
	 * @param attribute
	 * 		The metadata name.
	 * @param value
	 * 		The metadata value
	 */
	public AVUMetaData(String attribute, String value){
		this.attribute = attribute;
		this.value = value;
		this.unit = null;
	}
	
	/**
	 * Constructor with attribute, value and unit
	 * 
	 * @param attribute
	 * 		The metadata name.
	 * @param value
	 * 		The metadata value
	 * @param unit
	 * 		The metadata unit
	 */
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
	
	/**
     * This returns the metadata properties on a readable format. Can be used for logging
     * 
     * @return
     * 		The string representing the metadata
	 */
	@Override
    public String toString()
    {
        StringBuilder sb= new StringBuilder();
        sb.append(value);
        if (unit!=null){ sb.append(unit);}
        return sb.toString();

    }


}
