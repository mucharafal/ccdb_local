package ch.alice.o2.ccdb.servlets;

import ch.alice.o2.ccdb.RequestParser;

public class RequestParserImpl extends RequestParser {
    public RequestParserImpl(String path) {
        super(null);
        this.path = path;
    }

}
