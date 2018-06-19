package com.klst.mierp.process;

import org.compiere.model.MClient;
import org.compiere.model.MUser;
import org.compiere.process.SvrProcess;
import org.compiere.util.AdempiereUserError;
import org.compiere.util.EMail;
import org.compiere.util.Env;

//die beiden Methoden prepare() und doIt() werden nachwievor in den Subklassen implementiert
//daher : abstract
public abstract class SvrWfProcess extends SvrProcess {

	// aus SvrProcess:
	// protected CLogger			log = CLogger.getCLogger (getClass());

	public SvrWfProcess() {
		log.config("ctor");
	}

	@Override
	abstract protected void prepare();

	@Override
	abstract protected String doIt() throws Exception;

    private StringBuilder sbMsg = new StringBuilder(800); // capacity: 10 Zeilen a 80 char
    
    protected String getMsg() {
    	return this.sbMsg.toString();
    }
    
    // Problem
	// wenn call als process, dann werden Meldungen an gui geschickt
	// bei wf nicht, process task liefert nur true => wf weiterhin running oder false => suspend
    // daher schicke ich eine email
	protected String raiseError(String msg, String hint) throws AdempiereUserError {
		log.warning(msg + " Tipp: "+hint);
		if(this.getTable_ID()==0 && this.getRecord_ID()==0) { // call als process
			throw new AdempiereUserError(msg + "<br/> Tipp: " + hint);
		}
		// wf
		MClient mClient = new MClient(this.getCtx(), this.getAD_Client_ID(), this.get_TrxName());
		MUser mUser = new MUser(this.getCtx(), Env.getAD_User_ID(this.getCtx()), this.get_TrxName());
//		log.warning(" mClient: "+mClient + " mUser: "+mUser);
		mUser.getEMail();
		String subject = "ADempiere workflow terminated";
		StringBuilder content = new StringBuilder(800);
		content.append("Hallo,");
		content.append("\nwährend der automatischen workflow Verarbeitung ist ein Fehler aufgetreten - sorry, das sollte nicht passieren!");
		content.append("\n\nUrsache: \n");
		content.append(msg);
		content.append("\n\nTipp: \n");
		content.append(hint);
		content.append("\n\n");
		content.append(getClass());
		content.append("\nFalls Sie mit dieser Nachricht nichts anfangen können, senden Sie die mail weiter an klst.com - dort werden Sie geholfen! \n");
		EMail eMail = new EMail(mClient, mUser.getEMail(), mUser.getEMail(), subject, content.toString());
		log.config("send eMail - " + eMail.send());
		throw new AdempiereUserError(msg,hint);
	}

    protected String sendMsg(String m) {
		if(this.getTable_ID()==0 && this.getRecord_ID()==0) { // call als process
			return m;
		}
		// wf
		MClient mClient = new MClient(this.getCtx(), this.getAD_Client_ID(), this.get_TrxName());
		MUser mUser = new MUser(this.getCtx(), Env.getAD_User_ID(this.getCtx()), this.get_TrxName());
		mUser.getEMail();
		String subject = "ADempiere workflow beendet";
		StringBuilder content = new StringBuilder(800);
		content.append("Hallo,");
		content.append("\ndie automatischen workflow Verarbeitung ist abgeschlossen");
		content.append("\n\ndas wurde für Sie erledigt: \n\n");
		content.append(m.replaceAll("<br/>", "\n"));
		content.append("\n\n");
		content.append(getClass());
		content.append("\nFalls Sie mit dieser Nachricht nichts anfangen können, fragen Sie klst.com - dort werden Sie geholfen! \n");
		EMail eMail = new EMail(mClient, mUser.getEMail(), mUser.getEMail(), subject, content.toString());
		log.config("send eMail - " + eMail.send());
		return m;
	}

    protected void setMsg(String m) {
		log.warning(m);
		this.sbMsg.append("<br/>").append(m);
	}

}
