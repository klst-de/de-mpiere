package com.klst.opentrans;

import java.util.Properties;

import org.compiere.model.I_C_UOM;
import org.compiere.model.MUOM;

public class MUoM extends MUOM {

	private static final long serialVersionUID = 3601334593020526078L;

	public static MUOM getOrCreate(Properties ctx, String name, String trxName) {
		MUOM unit = get(ctx, name, trxName);
		if(unit==null) {
			unit = new MUoM(ctx, name, trxName);
			if(unit.save()) {
				unit.load(trxName);
			}
		}
		return unit;
	}
	
	public MUoM(Properties ctx, int C_UOM_ID, String trxName) {
		super(ctx, C_UOM_ID, trxName);
	}

	public MUoM(Properties ctx, String unit, String trxName) {
		super(ctx, 0, trxName);
		this.set_Value(I_C_UOM.COLUMNNAME_X12DE355, unit);
		this.set_Value(I_C_UOM.COLUMNNAME_Name, unit);
		this.set_Value(I_C_UOM.COLUMNNAME_Description, "used by openTRANS");
	}

}
