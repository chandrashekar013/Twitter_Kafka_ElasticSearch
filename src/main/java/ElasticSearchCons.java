import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ElasticSearchCons {

    public static RestHighLevelClient createClient() {

        //////////////////////////
        /////////// IF YOU USE LOCAL ELASTICSEARCH
        //////////////////////////

        //  String hostname = "localhost";
        //  RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,9200,"http"));


        //////////////////////////
        /////////// IF YOU USE BONSAI / HOSTED ELASTICSEARCH
        //////////////////////////

        // replace with your own credentials
        String hostname = ""; // localhost or bonsai url

        // needed only for bonsai
        String username = "";
        String password = ""; // needed only for bonsai

        // credentials provider help supply username and password
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }


    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(ElasticSearchCons.class);

        // To connect to ElasticSearch and perform Search operations
        RestHighLevelClient rc = createClient();
        String jsonString = "{ \"foo\": \"bar\"}";
        IndexRequest indexRequest = new IndexRequest(
                "twitter",
                "tweets").source(jsonString, XContentType.JSON);

        IndexResponse ir = rc.index(indexRequest, RequestOptions.DEFAULT);
        String id = ir.getId();
        log.info(id);

        rc.close();
    }
}
