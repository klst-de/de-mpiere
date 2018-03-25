package com.klst.opentrans;

public class TransformationException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public TransformationException(String message) {
		      super(message);
		   }

	   public TransformationException(String message, Throwable throwable) {
		      super(message, throwable);
		   }

}
