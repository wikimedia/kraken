package org.wikimedia.analytics.kraken.pageview;

import org.wikimedia.analytics.dclassjni.DeviceClassification;
import org.wikimedia.analytics.kraken.schemas.AppleUserAgent;
import org.wikimedia.analytics.kraken.schemas.Schema;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.wikimedia.analytics.kraken.schemas.JsonToClassConverter;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

public class UserAgent {

    //Additional Apple device recognizers
    private static final Pattern APPLE_BUILD_ID_PAT = Pattern.compile("(\\d{1,2}[A-L]\\d{1,3}a?)");
    private static HashMap<String, Schema> APPLE_UA_PATTERNS;
    static {
        JsonToClassConverter converter = new JsonToClassConverter();
        try {
            APPLE_UA_PATTERNS = converter.construct("org.wikimedia.analytics.kraken.schemas.AppleUserAgent", "ios.json", "getProduct");
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (JsonParseException e) {
            e.printStackTrace();
        }
        if (APPLE_UA_PATTERNS == null) {
            APPLE_UA_PATTERNS = new HashMap<String, Schema>();
        }
    }

    // Wikimedia Mobile Apps regular expressions
    // See: http://www.mediawiki.org/wiki/Mobile/User_agents
    private static final Pattern WMF_APP_ANDROID_UA_PAT = Pattern.compile("WikipediaMobile/.*Android.*");
    private static final Pattern WMF_APP_IOS            = Pattern.compile("Mozilla/5.0(?!.*\\bSafari\\b).*iPhone(?!.*\\bSafari\\b).*");
    //private static final Pattern WMF_APP_FIREFOX_UA_PAT = Pattern.compile("Mozilla/5.0 \\(Mobile; rv:.*\\) Gecko/.* Firefox/.*");
    private static final Pattern WMF_APP_FIREFOX_UA_PAT = Pattern.compile(Pattern.quote("Mozilla/5.0 (Mobile; rv:18.0) Gecko/18.0 Firefox/18.0"));
    private static final Pattern WMF_APP_RIM_UA_PAT     = Pattern.compile("Mozilla/5.0 \\(PlayBook; U; RIM Tablet OS.*\\)");
    //private static final Pattern WMF_APP_WINDOWS_UA_PAT = Pattern.compile("Mozilla/5.0 \\(compatible; MSIE 10.0; Windows NT.*\\)");
    private static final Pattern WMF_APP_WINDOWS_UA_PAT = Pattern.compile(Pattern.quote("Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0; MSAppHost/1.0)"));

    private static final Map<String, Pattern> WMF_APP_PATTERNS = new HashMap<String, Pattern>();
    static {
        WMF_APP_PATTERNS.put("Android",             WMF_APP_ANDROID_UA_PAT);
        WMF_APP_PATTERNS.put("Firefox OS",          WMF_APP_FIREFOX_UA_PAT);
        WMF_APP_PATTERNS.put("BlackBerry PlayBook", WMF_APP_RIM_UA_PAT);
        WMF_APP_PATTERNS.put("Windows 8",           WMF_APP_WINDOWS_UA_PAT);
        WMF_APP_PATTERNS.put("iOS",                 WMF_APP_IOS);
    }


    private String userAgent;
    private DeviceClassification deviceClassification;

    // Overrides and patches to dClass results
    private String model;
    private String deviceOsVersion;
    private String deviceClass;
    private String wmfMobileApp;



    public UserAgent(String ua) throws UnsupportedEncodingException {
        checkNotNull(ua);
        deviceClassification = new DeviceClassification();
        // userAgent = URLDecoder.decode(ua, "utf-8");
        userAgent = unspace(ua);
        classifyUserAgent();
    }


    private void classifyUserAgent(){
        deviceClassification.classifyUseragent(userAgent);

        model = deviceClassification.getModel();
        deviceOsVersion = deviceClassification.getDeviceOsVersion();

        final String vendor = deviceClassification.getVendor();
        final String dcModel = model;
        if ("Apple".equals(vendor)) {
            postProcessApple();
        // } else if ("Samsung".equals(vendor)) {
        //    model = postProcessSamsung(dcModel);
        }

        detectMobileApp();
    }

    /**
     * dClass has identified the mobile device as one from Apple but unfortunately
     * it does not provide reliable iOS version information. This function
     * adds iOS information but care should be used when this data is interpreted:
     * The iOS version is determined using the build number and hence the iOS field
     * should be read as "this mobile device has at least iOS version xyz running".
     */
    private void postProcessApple() {
        final String dcModel = deviceClassification.getIsTablet() ? "iPad" : deviceClassification.getModel();

        final Matcher match = APPLE_BUILD_ID_PAT.matcher(userAgent);
        if (dcModel == null || !match.find()) return;

        final String build = match.group(0).toString();
        final String key = dcModel.split(" ")[0] + "-" + build;
        final AppleUserAgent appleUserAgent = (AppleUserAgent) APPLE_UA_PATTERNS.get(key);
        if (appleUserAgent != null) {
            model = appleUserAgent.getAppleProduct();
            deviceOsVersion = appleUserAgent.getIOSVersion();
        }
    }

    /**
     * This function takes a Samsung model (GT-S5750E, GT S5620) and drops all
     * suffix characters and digits to allow for rollup of the keys.
     */
    private void postProcessSamsung() {
        final String dcModel = deviceClassification.getModel();
        final Matcher m = WMF_APP_ANDROID_UA_PAT.matcher(dcModel);
        if (m.matches() && m.groupCount() == 4) {
            final String name = m.group(1);
            final String value = m.group(3);
            final String valueCleaned = value.replaceAll("\\d", "");
            model = name + "-" + valueCleaned;
        } else {
            model = m.group(0) != null ? m.group(0) : dcModel;
        }
    }

    /**
     * If the useragent string is not identified as a mobile device using dClass
     * then we need to determine whether it's an Wikimedia mobile app. This
     * function iterates over a list of regular expressions to look for a match.
     */
    private void detectMobileApp() {
        Pattern pattern;
        for (Map.Entry<String, Pattern> entry : WMF_APP_PATTERNS.entrySet()) {
            pattern = entry.getValue();
            if (pattern.matcher(userAgent).find()) {
                wmfMobileApp = entry.getKey();
                return;
            }
        }
    }


    public String getParentId() {
        return deviceClassification.getParentId();
    }

    public String getVendor() {
        return deviceClassification.getVendor();
    }

    public String getModel() {
        return model;
    }

    public String getDeviceOs() {
        return deviceClassification.getDeviceOs();
    }

    public String getDeviceOsVersion() {
        return deviceOsVersion;
    }

    public String getBrowser() {
        return deviceClassification.getBrowser();
    }

    public String getBrowserVersion() {
        return deviceClassification.getBrowserVersion();
    }

    public boolean isHandset() {
        return deviceClassification.getIsWirelessDevice();
    }

    public boolean isTablet() {
        return deviceClassification.getIsTablet();
    }

    public boolean isDesktop() {
        return deviceClassification.getIsDesktop();
    }

    public boolean isCrawler() {
        return deviceClassification.getIsCrawler();
    }

    public String getDeviceClass() {
        if (deviceClass == null) {
            deviceClass =
                (isHandset() ? "handheld" :
                (isTablet()  ? "tablet"   :
                (isCrawler() ? "crawler"  :
                (isDesktop() ? "desktop"  :
                               "unknown"  ))));
        }
        return deviceClass;
    }

    public String getWMFMobileApp() {
        return wmfMobileApp;
    }

    public String getInputDevices() {
        return deviceClassification.getInputDevices();
    }

    public boolean getAjaxSupportJavascript() {
        return deviceClassification.getAjaxSupportJavascript();
    }

    public int getDisplayWidth() {
        return deviceClassification.getDisplayWidth();
    }

    public int getDisplayHeight() {
        return deviceClassification.getDisplayHeight();
    }

    public String getDisplayDimensions() {
        return getDisplayWidth() + " x " + getDisplayHeight();
    }


    @Override
    public String toString() {
        return userAgent;
    }

    private static String unspace(final String s) {
        return s.replace("%20", " ");
    }
}
