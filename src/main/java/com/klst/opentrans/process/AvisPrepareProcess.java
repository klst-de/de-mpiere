package com.klst.opentrans.process;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

/*
 * wird nicht von SvrProcess abgeleitet, da mit CreateProductProcess vieles gemeinsam genutzt wird
 * 
 * prepare() und doIt() aus super!
 */
/**
 * die Funktionalität ist nicht in doOne(), sondern in movefile(...)
 */
public class AvisPrepareProcess extends CreateProductProcess {
	
	/*
	 * Perform process (aus super) verarbeitet mehrere files und ruft doOne für ein opentrans-ORDER
	 * 
	 * @return Message
	 * @throws Exception
	 */	
	@Override
	protected String doOne(String msg, String uri) throws Exception {
		
		String ret = uri;
		log.info("nix tun uri="+uri );
		return ret;
	}

	@Override
	protected boolean movefile(File src, File tgt) {
		try {
			AvisPipedInputStream is = new AvisPipedInputStream(src.getAbsolutePath(), null); // charsetName=null wg. #343
			OutputStreamWriter fw = new OutputStreamWriter(new FileOutputStream(tgt), Charset.forName("UTF-8"));
			int c = is.read();
			do {
				fw.write(c);
				c = is.read();
			} while(c!=-1);
			fw.close();
			is.close();
			//return true; // derzeit macht die Methode eine Kopie, also kein move!
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

}
