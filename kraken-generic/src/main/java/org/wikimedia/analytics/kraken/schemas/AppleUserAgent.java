package org.wikimedia.analytics.kraken.schemas;


public class AppleUserAgent extends Schema {

    public String Product;
    public String AppleProduct;
    public String UserAgentPrefix;
    public String Build;
    public String Introduced;
    public String IOSVersion;


    public String getProduct() {
        return Product;
    }

    public String getAppleProduct() {
        return AppleProduct;
    }

    public String getUserAgentPrefix() {
        return UserAgentPrefix;
    }

    public String getBuild() {
        return Build;
    }

    public String getIntroduced() {
        return Introduced;
    }

    public String getIOSVersion() {
        return IOSVersion;
    }



}
