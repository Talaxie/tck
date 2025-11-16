/*
 * Développé par : Hervé Ciaravolo / Red-belt
 */
package com.talaxie.components.output;

import java.util.ResourceBundle;

public class Messages {

    private static final ResourceBundle BUNDLE =
            ResourceBundle.getBundle("com.talaxie.components.output.Messages");

    public static String errorDeleteFailed(String path) {
        return BUNDLE.getString("errorDeleteFailed").replace("{0}", path);
    }

    public static String errorFileExists(String path) {
        return BUNDLE.getString("errorFileExists").replace("{0}", path);
    }


}