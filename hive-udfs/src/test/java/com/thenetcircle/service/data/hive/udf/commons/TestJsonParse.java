package com.thenetcircle.service.data.hive.udf.commons;

import com.google.gson.JsonObject;
import org.json.JSONObject;

public class TestJsonParse {
    public static void main(String[] args) {
        String testStr = "{\n" +
                "            \"`8459`\": {\n" +
                "                \"id\": 8459,\n" +
                "                \"email\": \"clayder@freenet.de\",\n" +
                "                \"is_primary\": true,\n" +
                "                \"is_validated\": true,\n" +
                "                \"is_notification_enabled\": false,\n" +
                "                \"created_at\": \"2017-04-13T10:03:38+00:00\",\n" +
                "                \"updated_at\": \"2020-05-30T18:31:37+00:00\",\n" +
                "                \"resource_type\": \"identity_email\",\n" +
                "                \"resource_url\": \"https://helpdeskkaufmich.kayako.com/api/v1/identities/emails/8459\"\n" +
                "            },\n" +
                "            \"10049\": {\n" +
                "                \"id\": 10049,\n" +
                "                \"email\": \"j.horn87@web.de\",\n" +
                "                \"is_primary\": true,\n" +
                "                \"is_validated\": true,\n" +
                "                \"is_notification_enabled\": false,\n" +
                "                \"created_at\": \"2017-04-16T05:40:12+00:00\",\n" +
                "                \"updated_at\": \"2017-08-31T20:20:21+00:00\",\n" +
                "                \"resource_type\": \"identity_email\",\n" +
                "                \"resource_url\": \"https://helpdeskkaufmich.kayako.com/api/v1/identities/emails/10049\"\n" +
                "            },\n" +
                "            \"19636\": {\n" +
                "                \"id\": 19636,\n" +
                "                \"email\": \"c511019@mvrht.com\",\n" +
                "                \"is_primary\": true,\n" +
                "                \"is_validated\": true,\n" +
                "                \"is_notification_enabled\": false,\n" +
                "                \"created_at\": \"2017-05-03T19:54:44+00:00\",\n" +
                "                \"updated_at\": \"2020-05-31T13:46:39+00:00\",\n" +
                "                \"resource_type\": \"identity_email\",\n" +
                "                \"resource_url\": \"https://helpdeskkaufmich.kayako.com/api/v1/identities/emails/19636\"\n" +
                "            },\n" +
                "            \"36609\": {\n" +
                "                \"id\": 36609,\n" +
                "                \"email\": \"uwebastian72@gmx.de\",\n" +
                "                \"is_primary\": true,\n" +
                "                \"is_validated\": true,\n" +
                "                \"is_notification_enabled\": false,\n" +
                "                \"created_at\": \"2017-06-08T14:24:10+00:00\",\n" +
                "                \"updated_at\": \"2017-08-10T08:44:17+00:00\",\n" +
                "                \"resource_type\": \"identity_email\",\n" +
                "                \"resource_url\": \"https://helpdeskkaufmich.kayako.com/api/v1/identities/emails/36609\"\n" +
                "            },\n" +
                "            \"37385\": {\n" +
                "                \"id\": 37385,\n" +
                "                \"email\": \"meask212@web.de\",\n" +
                "                \"is_primary\": true,\n" +
                "                \"is_validated\": true,\n" +
                "                \"is_notification_enabled\": false,\n" +
                "                \"created_at\": \"2017-06-10T06:04:50+00:00\",\n" +
                "                \"updated_at\": \"2018-03-21T11:09:49+00:00\",\n" +
                "                \"resource_type\": \"identity_email\",\n" +
                "                \"resource_url\": \"https://helpdeskkaufmich.kayako.com/api/v1/identities/emails/37385\"\n" +
                "            },\n" +
                "            \"41602\": {\n" +
                "                \"id\": 41602,\n" +
                "                \"email\": \"geweer@web.de\",\n" +
                "                \"is_primary\": true,\n" +
                "                \"is_validated\": true,\n" +
                "                \"is_notification_enabled\": false,\n" +
                "                \"created_at\": \"2017-06-18T16:18:39+00:00\",\n" +
                "                \"updated_at\": \"2017-10-08T11:38:18+00:00\",\n" +
                "                \"resource_type\": \"identity_email\",\n" +
                "                \"resource_url\": \"https://helpdeskkaufmich.kayako.com/api/v1/identities/emails/41602\"\n" +
                "            },\n" +
                "            \"52712\": {\n" +
                "                \"id\": 52712,\n" +
                "                \"email\": \"bloodangel3@web.de\",\n" +
                "                \"is_primary\": true,\n" +
                "                \"is_validated\": true,\n" +
                "                \"is_notification_enabled\": false,\n" +
                "                \"created_at\": \"2017-07-14T15:43:19+00:00\",\n" +
                "                \"updated_at\": \"2018-04-11T04:03:57+00:00\",\n" +
                "                \"resource_type\": \"identity_email\",\n" +
                "                \"resource_url\": \"https://helpdeskkaufmich.kayako.com/api/v1/identities/emails/52712\"\n" +
                "            },\n" +
                "            \"59968\": {\n" +
                "                \"id\": 59968,\n" +
                "                \"email\": \"timo.stuenkel@gmx.de\",\n" +
                "                \"is_primary\": true,\n" +
                "                \"is_validated\": true,\n" +
                "                \"is_notification_enabled\": false,\n" +
                "                \"created_at\": \"2017-08-01T18:03:48+00:00\",\n" +
                "                \"updated_at\": \"2017-10-22T15:08:45+00:00\",\n" +
                "                \"resource_type\": \"identity_email\",\n" +
                "                \"resource_url\": \"https://helpdeskkaufmich.kayako.com/api/v1/identities/emails/59968\"\n" +
                "            },\n" +
                "            \"62614\": {\n" +
                "                \"id\": 62614,\n" +
                "                \"email\": \"luhilge@web.de\",\n" +
                "                \"is_primary\": true,\n" +
                "                \"is_validated\": true,\n" +
                "                \"is_notification_enabled\": false,\n" +
                "                \"created_at\": \"2017-08-08T18:58:58+00:00\",\n" +
                "                \"updated_at\": \"2017-08-08T18:58:58+00:00\",\n" +
                "                \"resource_type\": \"identity_email\",\n" +
                "                \"resource_url\": \"https://helpdeskkaufmich.kayako.com/api/v1/identities/emails/62614\"\n" +
                "            },\n" +
                "            \"69490\": {\n" +
                "                \"id\": 69490,\n" +
                "                \"email\": \"dasii@hotmail.de\",\n" +
                "                \"is_primary\": true,\n" +
                "                \"is_validated\": true,\n" +
                "                \"is_notification_enabled\": false,\n" +
                "                \"created_at\": \"2017-08-28T07:20:11+00:00\",\n" +
                "                \"updated_at\": \"2017-08-28T07:20:12+00:00\",\n" +
                "                \"resource_type\": \"identity_email\",\n" +
                "                \"resource_url\": \"https://helpdeskkaufmich.kayako.com/api/v1/identities/emails/69490\"\n" +
                "            },\n" +
                "            \"80300\": {\n" +
                "                \"id\": 80300,\n" +
                "                \"email\": \"schlacky66@googlemail.com\",\n" +
                "                \"is_primary\": true,\n" +
                "                \"is_validated\": true,\n" +
                "                \"is_notification_enabled\": false,\n" +
                "                \"created_at\": \"2017-09-26T18:47:36+00:00\",\n" +
                "                \"updated_at\": \"2017-09-26T18:47:36+00:00\",\n" +
                "                \"resource_type\": \"identity_email\",\n" +
                "                \"resource_url\": \"https://helpdeskkaufmich.kayako.com/api/v1/identities/emails/80300\"\n" +
                "            },\n" +
                "            \"90024\": {\n" +
                "                \"id\": 90024,\n" +
                "                \"email\": \"manfredarte@googlemail.com\",\n" +
                "                \"is_primary\": true,\n" +
                "                \"is_validated\": true,\n" +
                "                \"is_notification_enabled\": false,\n" +
                "                \"created_at\": \"2017-10-23T20:31:13+00:00\",\n" +
                "                \"updated_at\": \"2017-10-23T20:31:13+00:00\",\n" +
                "                \"resource_type\": \"identity_email\",\n" +
                "                \"resource_url\": \"https://helpdeskkaufmich.kayako.com/api/v1/identities/emails/90024\"\n" +
                "            },\n" +
                "            \"91111\": {\n" +
                "                \"id\": 91111,\n" +
                "                \"email\": \"rehm-steven@web.de\",\n" +
                "                \"is_primary\": true,\n" +
                "                \"is_validated\": true,\n" +
                "                \"is_notification_enabled\": false,\n" +
                "                \"created_at\": \"2017-10-26T20:11:09+00:00\",\n" +
                "                \"updated_at\": \"2017-10-26T20:11:09+00:00\",\n" +
                "                \"resource_type\": \"identity_email\",\n" +
                "                \"resource_url\": \"https://helpdeskkaufmich.kayako.com/api/v1/identities/emails/91111\"\n" +
                "            },\n" +
                "            \"111576\": {\n" +
                "                \"id\": 111576,\n" +
                "                \"email\": \"albert1888@web.de\",\n" +
                "                \"is_primary\": true,\n" +
                "                \"is_validated\": true,\n" +
                "                \"is_notification_enabled\": false,\n" +
                "                \"created_at\": \"2017-12-26T15:20:23+00:00\",\n" +
                "                \"updated_at\": \"2017-12-26T15:20:23+00:00\",\n" +
                "                \"resource_type\": \"identity_email\",\n" +
                "                \"resource_url\": \"https://helpdeskkaufmich.kayako.com/api/v1/identities/emails/111576\"\n" +
                "            },\n" +
                "            \"113963\": {\n" +
                "                \"id\": 113963,\n" +
                "                \"email\": \"sabo82@aol.com\",\n" +
                "                \"is_primary\": true,\n" +
                "                \"is_validated\": true,\n" +
                "                \"is_notification_enabled\": false,\n" +
                "                \"created_at\": \"2018-01-02T17:48:54+00:00\",\n" +
                "                \"updated_at\": \"2018-01-02T17:48:54+00:00\",\n" +
                "                \"resource_type\": \"identity_email\",\n" +
                "                \"resource_url\": \"https://helpdeskkaufmich.kayako.com/api/v1/identities/emails/113963\"\n" +
                "            },\n" +
                "            \"123370\": {\n" +
                "                \"id\": 123370,\n" +
                "                \"email\": \"theprettypants@gmail.com\",\n" +
                "                \"is_primary\": true,\n" +
                "                \"is_validated\": true,\n" +
                "                \"is_notification_enabled\": false,\n" +
                "                \"created_at\": \"2018-01-30T10:39:21+00:00\",\n" +
                "                \"updated_at\": \"2018-01-30T10:39:21+00:00\",\n" +
                "                \"resource_type\": \"identity_email\",\n" +
                "                \"resource_url\": \"https://helpdeskkaufmich.kayako.com/api/v1/identities/emails/123370\"\n" +
                "            },\n" +
                "            \"128024\": {\n" +
                "                \"id\": 128024,\n" +
                "                \"email\": \"knallfrosch36@freenet.de\",\n" +
                "                \"is_primary\": true,\n" +
                "                \"is_validated\": true,\n" +
                "                \"is_notification_enabled\": false,\n" +
                "                \"created_at\": \"2018-02-11T16:38:37+00:00\",\n" +
                "                \"updated_at\": \"2018-02-11T16:38:38+00:00\",\n" +
                "                \"resource_type\": \"identity_email\",\n" +
                "                \"resource_url\": \"https://helpdeskkaufmich.kayako.com/api/v1/identities/emails/128024\"\n" +
                "            },\n" +
                "            \"164550\": {\n" +
                "                \"id\": 164550,\n" +
                "                \"email\": \"thomas_brodi@live.de\",\n" +
                "                \"is_primary\": true,\n" +
                "                \"is_validated\": true,\n" +
                "                \"is_notification_enabled\": false,\n" +
                "                \"created_at\": \"2018-05-24T17:14:26+00:00\",\n" +
                "                \"updated_at\": \"2018-05-24T17:14:57+00:00\",\n" +
                "                \"resource_type\": \"identity_email\",\n" +
                "                \"resource_url\": \"https://helpdeskkaufmich.kayako.com/api/v1/identities/emails/164550\"\n" +
                "            },\n" +
                "            \"222728\": {\n" +
                "                \"id\": 222728,\n" +
                "                \"email\": \"klassisch66@gmx.de\",\n" +
                "                \"is_primary\": true,\n" +
                "                \"is_validated\": true,\n" +
                "                \"is_notification_enabled\": false,\n" +
                "                \"created_at\": \"2018-09-21T07:11:30+00:00\",\n" +
                "                \"updated_at\": \"2018-09-21T07:11:30+00:00\",\n" +
                "                \"resource_type\": \"identity_email\",\n" +
                "                \"resource_url\": \"https://helpdeskkaufmich.kayako.com/api/v1/identities/emails/222728\"\n" +
                "            },\n" +
                "            \"229594\": {\n" +
                "                \"id\": 229594,\n" +
                "                \"email\": \"dragonace@t-online.de\",\n" +
                "                \"is_primary\": true,\n" +
                "                \"is_validated\": true,\n" +
                "                \"is_notification_enabled\": false,\n" +
                "                \"created_at\": \"2018-10-03T10:36:56+00:00\",\n" +
                "                \"updated_at\": \"2018-10-03T10:36:56+00:00\",\n" +
                "                \"resource_type\": \"identity_email\",\n" +
                "                \"resource_url\": \"https://helpdeskkaufmich.kayako.com/api/v1/identities/emails/229594\"\n" +
                "            }\n" +
                "        }";
        JSONObject extractObject = new JSONObject(testStr);


        JsonObject jsonObject = new JsonObject();
        jsonObject.getAsJsonObject(testStr);
    }
}
