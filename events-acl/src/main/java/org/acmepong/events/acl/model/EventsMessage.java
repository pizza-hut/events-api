package org.acmepong.events.acl.model;

import java.util.Date;

public class EventsMessage {

	private String message;
	private String sender;
	private Date receivedDatetime;
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public String getSender() {
		return sender;
	}
	public void setSender(String sender) {
		this.sender = sender;
	}
	public Date getReceivedDatetime() {
		return receivedDatetime;
	}
	public void setReceivedDatetime(Date receivedDatetime) {
		this.receivedDatetime = receivedDatetime;
	}
	
}
