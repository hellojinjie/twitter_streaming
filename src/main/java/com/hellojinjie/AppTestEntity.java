package com.hellojinjie;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Hello world!
 */
public class AppTestEntity {
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
        List<Long> userList = Lists.newArrayList(718433555156566016l);
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

        Set<String> usersReturnedByTwitter = Sets.newHashSet();

        // Do whatever needs to be done with messages
        for (int msgRead = 0; msgRead < 1000; msgRead++) {
            String msg = queue.take();

            Map<String, Object> tweet = objectMapper.readValue(msg, Map.class);
            System.out.println("=============================================");

            ObjectMapper mapper = new ObjectMapper();
            Object json = mapper.readValue(msg, Object.class);
            mapper.enable(SerializationFeature.INDENT_OUTPUT);
            String indented = mapper.writeValueAsString(json);
            System.out.println(indented);
        }

        client.stop();

    }
}
