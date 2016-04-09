package com.hellojinjie;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws Exception {
        run("56RQVuss2awvlojRVtJSKZi9d",
                "gPmG6LWp2V5AFF0KFYcZKIlsU9YKmVDdKVl6P4chF5DkcYFAJq",
                "140890886-ihPVjuQ15zEZpDuuf6bBARwIuvJNxU9GlwPj0Aq2",
                "xiekxrD1dFyCfceN0nQzo4u6SQ6zyk8s6m2t8S1tqW8EE");
    }

    public static void run(String consumerKey, String consumerSecret, String token, String secret) throws Exception {
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        // add some track terms
        List<Long> userList = Lists.newArrayList(2751538172l, 912817236l, 60140216l, 2712540668l, 101005282l, 3186330252l, 3223510129l, 395115784l, 3370281612l, 4804158733l, 430519990l, 3310685269l, 1945813231l, 2177706959l, 485769605l, 118737759l, 2361009858l, 588261604l, 36325225l, 36324632l, 81091668l, 4898091l, 2097571l, 36320862l, 28140646l, 22205952l, 29959949l);
        endpoint.followings(userList);

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
        // Authentication auth = new BasicAuth(username, password);

        // Create a new BasicClient. By default gzip is enabled.
        Client client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        // Establish a connection
        client.connect();

        ObjectMapper objectMapper = new ObjectMapper();

        // Do whatever needs to be done with messages
        for (int msgRead = 0; msgRead < 1000; msgRead++) {
            String msg = queue.take();

            Map<String, Object> tweet = objectMapper.readValue(msg, Map.class);
            System.out.println("=============================================");
            System.out.println(tweet.get("text"));
            Map<String, Object> user = (Map<String, Object>) tweet.get("user");
            System.out.println(user.get("screen_name"));
            System.out.println(user.get("id"));
            System.out.println("Is in user list: " + userList.contains(user.get("id")));
        }

        client.stop();

    }
}
