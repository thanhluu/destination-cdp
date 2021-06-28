package io.airbyte.integrations.destination.cdp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.base.FailureTrackingAirbyteMessageConsumer;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

public class CdpConsumer extends FailureTrackingAirbyteMessageConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(CdpConsumer.class);
    private final JsonNode config;
    private final ConfiguredAirbyteCatalog catalog;
    private final Consumer<AirbyteMessage> outputRecordCollector;
    private CDPAccount account;

    public CdpConsumer(JsonNode config,
                          ConfiguredAirbyteCatalog catalog,
                          Consumer<AirbyteMessage> outputRecordCollector) {
        this.outputRecordCollector = outputRecordCollector;
        this.config = config;
        this.catalog = catalog;
        LOGGER.info("initializing consumer.");
    }

    @Override
    protected void startTracked() throws Exception {
        String authUrl = config.get("cdp_host").asText() + "auth/auth/login";
        String username = config.get("username").asText();
        String password = config.get("password").asText();

        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(authUrl);
        JsonNode data = Jsons.jsonNode(ImmutableMap.builder()
                .put("username", username)
                .put("password", password)
                .build()
        );
        LOGGER.info("JsonNode: " + data.toString());
        StringEntity entity = new StringEntity(data.toString(),"utf-8");
        entity.setContentType(new BasicHeader("Content-type", "application/json"));

        httpPost.setEntity(entity);
        httpPost.setHeader("Accept", "application/json");
        httpPost.setHeader("Content-type", "application/json");
        CloseableHttpResponse response = client.execute(httpPost);
        HttpEntity resEntity = response.getEntity();
        String retSrc = EntityUtils.toString(resEntity);
        LOGGER.info("retSrc: " + retSrc);
        client.close();

        ObjectMapper mapper = new ObjectMapper();
        JsonNode result = mapper.readTree(retSrc);

        LOGGER.info("result: " + result.toString());
        LOGGER.info("config: " + config.toString());

        this.account = new CDPAccount(
                result.get("id").asText(),
                config.get("workspace_id").asText(),
                result.get("accessToken").asText());
    }

    @Override
    protected void acceptTracked(AirbyteMessage msg) throws Exception {
        if (!msg.getType().equals(AirbyteMessage.Type.RECORD)) {
            return;
        }
        JsonNode recordMessage = msg.getRecord().getData();
        String contactUrl = config.get("cdp_host").asText() + "contacts/contacts/";

        if (config.get("cdp_type").asText().equals("contacts")) {
            CloseableHttpClient httpClient = HttpClients.createDefault();
            ObjectMapper mapper = new ObjectMapper();

            String createContactDto = Jsons.jsonNode(ImmutableMap.builder()
                    .put("userId", this.account.getUserId())
                    .put("workspaceId", this.account.getWorkspaceId())
                    .put("email", recordMessage.get("email").asText().isEmpty() ? UUID.randomUUID() + "@dummy.com" : recordMessage.get("email").asText())
                    .put("firstName", recordMessage.get("agency_name").asText())
                    .put("phone", recordMessage.get("phone").asText())
                    .put("address1", recordMessage.get("address_1").asText())
                    .put("address2", recordMessage.get("address_2").asText())
                    .build()
            ).toString();
            LOGGER.info("createContactDto: " + createContactDto);

            HttpPost httpPost = new HttpPost(contactUrl);
            StringEntity postEntity = new StringEntity(createContactDto);
            postEntity.setContentType(new BasicHeader("Content-type", "application/json"));

            httpPost.setEntity(postEntity);
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-type", "application/json");
            httpPost.setHeader("Authorization", "Bearer " + this.account.getToken());

            String retCreate = EntityUtils.toString(httpClient.execute(httpPost).getEntity());
            LOGGER.info("retCreate: " + retCreate);
            String contactId = mapper.readTree(retCreate).get("id").asText();

            transformKeys(recordMessage);
            Map<String, String> fieldOptions = mapper.convertValue(recordMessage, Map.class);
            fieldOptions.remove("email");
            fieldOptions.remove("phone");
            fieldOptions.remove("address_1");
            fieldOptions.remove("address_2");
            fieldOptions.remove("agency_name");
            LOGGER.info("fieldOptions: " + String.join(", ", fieldOptions.keySet()));

            HttpPut httpPut = new HttpPut(contactUrl + contactId);
            StringEntity putEntity = new StringEntity(Jsons.jsonNode(fieldOptions).toString());
            putEntity.setContentType(new BasicHeader("Content-type", "application/json"));

            httpPut.setEntity(putEntity);
            httpPut.setHeader("Accept", "application/json");
            httpPut.setHeader("Content-type", "application/json");
            httpPut.setHeader("Authorization", "Bearer " + this.account.getToken());

            String retUpdate = EntityUtils.toString(httpClient.execute(httpPut).getEntity());
            LOGGER.info("retUpdate: " + retUpdate);
        }
    }

    @Override
    protected void close(boolean hasFailed) throws Exception {
        if (!hasFailed) {
            LOGGER.info("shutting down consumer.");
        }
    }

    private static class CDPAccount {
        private String userId;
        private String workspaceId;
        private String token;

        public CDPAccount(String userId, String workspaceId, String token) {
            this.userId = userId;
            this.workspaceId = workspaceId;
            this.token = token;
        }

        public String getUserId() {
            return userId;
        }

        public String getWorkspaceId() {
            return workspaceId;
        }

        public String getToken() {
            return token;
        }
    }

    private static void transformKeys(JsonNode parent) {
        if (parent.isObject()) {
            ObjectNode node = (ObjectNode) parent;

            List<String> names = new ArrayList<>();
            node.fieldNames().forEachRemaining(names::add);

            names.forEach(name -> {
                JsonNode item = node.remove(name);
                transformKeys(item);
                node.replace(name.toLowerCase(), item);
            });
        } else if (parent.isArray()) {
            ArrayNode array = (ArrayNode) parent;
            array.elements().forEachRemaining(CdpConsumer::transformKeys);
        }
    }
}
