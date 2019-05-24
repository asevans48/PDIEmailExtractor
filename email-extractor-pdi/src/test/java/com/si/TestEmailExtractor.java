package com.si;

import org.junit.jupiter.api.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestEmailExtractor {
    String regex = "(?mis)([a-zA-Z0-9_!#$%&'*+/=?`{|}~^\\.\\-]+@[a-zA-Z0-9\\.\\-]+)";
    private Pattern emailRegex = Pattern.compile(regex);

    @Test
    public void shouldPullValidEmailsFromText(){
        String[] emails = {"heine@yahoo.com", "flavell@msn.com", "melnik@gmail.com", "wagnerch@mac.com"};
        String text = "    heine@yahoo.com" +
                " is located at   flavell@msn.com" +
                "    melnik@gmail.com" +
                " was underneath   wagnerch@mac.com";
        Matcher m = emailRegex.matcher(text);
        int i = 0;
        while(m.find()){
            assert(m.groupCount() == 1);
            System.out.println(m.group(0));
            String email = m.group(0);
            assert(email.trim().equals(emails[i]));
            i += 1;
        }
        assert(i > 0);
    }
}
