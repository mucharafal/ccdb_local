package ch.alice.o2.ccdb.servlets;

import ch.alice.o2.ccdb.webserver.SQLBackedTomcat;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class SQLBackedHTTPTest implements FilledDatabaseForTest {
    private static HttpClient client;

    @BeforeAll
    static void init() throws Exception {
        client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        SQLBackedTomcat.main(new String[0]);
    }

    @Test
    public void givenAClient_whenEnteringBaeldung_thenPageTitleIsOk()
            throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8080/a/1/1000000000000"))
                .timeout(Duration.ofMinutes(1))
                .header("Content-Type", "application/json")
                .GET()
                .build();

        HttpResponse<String> response =
                client.send(request, HttpResponse.BodyHandlers.ofString());



    }
}
