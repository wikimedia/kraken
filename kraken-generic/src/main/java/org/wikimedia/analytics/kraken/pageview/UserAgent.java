package org.wikimedia.analytics.kraken.pageview;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.wikimedia.analytics.dclassjni.DeviceClassification;
import org.wikimedia.analytics.kraken.schemas.AppleUserAgent;
import org.wikimedia.analytics.kraken.schemas.JsonToClassConverter;
import org.wikimedia.analytics.kraken.schemas.Schema;

import java.io.UnsupportedEncodingException;
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
    private static final Pattern WMF_APP_ANDROID_UA_PAT     = Pattern.compile("WikipediaMobile/.*Android.*");
    private static final Pattern WMF_APP_IOS                = Pattern.compile("Mozilla/[0-9\\.]* \\((iPad|iPhone|iPod); CPU iPhone OS [0-9_]* like Mac OS X\\) AppleWebKit/[0-9\\.]* \\(KHTML, like Gecko\\) Mobile/.*");
    private static final Pattern WMF_APP_FIREFOX_UA_PAT     = Pattern.compile("Mozilla/[0-9\\.]* \\(Mobile; rv:[0-9\\.]*\\) Gecko/[0-9\\.]* Firefox/[0-9\\.]*");
    private static final Pattern WMF_APP_RIM_UA_PAT         = Pattern.compile("Mozilla/[0-9\\.]* \\(PlayBook; U; RIM Tablet OS.*\\)");
    private static final Pattern WMF_APP_WINDOWS_UA_PAT     = Pattern.compile("Mozilla/[0-9\\.]* \\(.*MSIE.*MSAppHost.*\\)");

    // Non-Wikimedia Mobile Apps regular expressions
    // See: http://www.mediawiki.org/wiki/Mobile/User_agents#Un-Official_Apps
    private static final Pattern NON_WMF_APP_Wikipanion     = Pattern.compile("Wikipanion/.*CFNetwork/.*Darwin/.*"); // example: Wikipanion/1.7.8.3.CFNetwork/609.1.4.Darwin/13.0.0
    private static final Pattern NON_WMF_APP_WikiBot        = Pattern.compile("Wikibot/.*CFNetwork/.*Darwin/.*"); // example: Wikibot/2.0.2.CFNetwork/609.1.4.Darwin/13.0.0
    private static final Pattern NON_WMF_APP_OnThisDay      = Pattern.compile("OnThisDay/.*CFNetwork/.*Darwin/.*"); // example: OnThisDay/48.CFNetwork/609.1.4.Darwin/13.0.0
    private static final Pattern NON_WMF_APP_Wikihood       = Pattern.compile("Wikihood (iPad|iPhone|iPod)/[0-9\\.]*"); // example: Wikihood iPad/1.3.3
    private static final Pattern NON_WMF_APP_WikiHunt       = Pattern.compile("WikiHunt/.*CFNetwork/.*Darwin/.*"); // example: WikiHunt/1.7.CFNetwork/609.1.4.Darwin/13.0.0
    private static final Pattern NON_WMF_APP_Articles       = Pattern.compile("Articles/.*CFNetwork/.*Darwin/.*"); // example: Articles/285.CFNetwork/609.1.4.Darwin/13.0.0
    // private static final Pattern NON_WMF_APP_iPediaWiki     = NOTE: UA too generic // example: Mozilla/5.0.(iPod;.CPU.iPhone.OS.6_1_3.like.Mac.OS.X).AppleWebKit/536.26.(KHTML,.like.Gecko).Mobile/10B329
    // private static final Pattern NON_WMF_APP_The Wiki game  = NOTE: UA too generic // example: Mozilla/5.0.(iPod;.CPU.iPhone.OS.6_1_3.like.Mac.OS.X).AppleWebKit/536.26.(KHTML,.like.Gecko).Mobile/10B329
    //private static final Pattern NON_WMF_APP_WikiTap        = //Proxy - 207.154.19.129
    //private static final Pattern NON_WMF_APP_Wapedia        = //Proxy - 82.147.11.31 - /en/Independence_Party_(Iceland)?applang=en&appsearchsite=en&appver=1.3.2&iapp_devtype=iPod%20touch&iapp_prefs=picturesize:on&iapp_res=6&sid=1493948423

    private static final Map<String, Pattern>
        WMF_APP_PATTERNS = new HashMap<String, Pattern>(),
        NON_WMF_APP_PATTERNS = new HashMap<String, Pattern>();
    static {
        // WMF apps
        WMF_APP_PATTERNS.put("Android",             WMF_APP_ANDROID_UA_PAT);
        WMF_APP_PATTERNS.put("Firefox OS",          WMF_APP_FIREFOX_UA_PAT);
        WMF_APP_PATTERNS.put("BlackBerry PlayBook", WMF_APP_RIM_UA_PAT);
        WMF_APP_PATTERNS.put("Windows 8",           WMF_APP_WINDOWS_UA_PAT);
        WMF_APP_PATTERNS.put("iOS",                 WMF_APP_IOS);

        // non-WMF apps
        NON_WMF_APP_PATTERNS.put("Wikipanion",      NON_WMF_APP_Wikipanion);
        NON_WMF_APP_PATTERNS.put("WikiBot",         NON_WMF_APP_WikiBot);
        NON_WMF_APP_PATTERNS.put("OnThisDay",       NON_WMF_APP_OnThisDay);
        NON_WMF_APP_PATTERNS.put("Wikihood",        NON_WMF_APP_Wikihood);
        NON_WMF_APP_PATTERNS.put("WikiHunt",        NON_WMF_APP_WikiHunt);
        NON_WMF_APP_PATTERNS.put("Articles",        NON_WMF_APP_Articles);
    }


    private String userAgent;
    private DeviceClassification deviceClassification;

    // Overrides and patches to dClass results
    private String model;
    private String deviceOsVersion;
    private String deviceClass;
    private String wmfMobileApp;
    private String nonWmfMobileApp;



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
        for (Map.Entry<String, Pattern> entry : NON_WMF_APP_PATTERNS.entrySet()) {
            pattern = entry.getValue();
            if (pattern.matcher(userAgent).find()) {
                nonWmfMobileApp = entry.getKey();
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

    public String getNonWMFMobileApp() {
        return nonWmfMobileApp;
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
