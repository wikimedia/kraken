/**
 * Copyright (C) 2012-2013  Wikimedia Foundation

 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package org.wikimedia.analytics.dclassjni;

import java.util.Iterator;
import java.util.Map;


import dclass.dClass;


/**
 * The class dClassParsedResult. This class is a mapping of the results from
 * dclass_keyvalue data structure to a Java data structure :)
 *
 * OpenDDR attributes => vendor: 'HTC'
 * model: 'A6380'
 * parentId: 'genericHTC'
 * inputDevices: 'touchscreen'
 * displayHeight: '480'
 * displayWidth: '320'
 * device_os: 'Android'
 * ajax_support_javascript: 'true'
 * is_tablet: 'false'
 * is_wireless_device: 'true'
 * is_crawler: 'false'
 * is_desktop: 'false'
 *
 */
public class DeviceClassification {
    public final static String dtree_path = "/usr/share/libdclass/openddr.dtree";
    /**
     * Only initialize the dClass JNI wrapper on-demand (and only once),
     * as doing so loads 2+ MB of dtree data into memory from disk.
     */


    /**
     * Wrapper subclass to release the dtree data when the singleton is
     * destroyed, presumably when its classloader is released (which *can*
     * happen without imminent JVM termination, such as in IOC containers,
     * like Spring).
     */
    final private static dClass dClass = new dClass(dtree_path);

    synchronized private static Map classifyUA(String ua) {

        return dClass.classify(ua);
    }



    private String parentId;
    private String vendor;
    private String model;
    private String deviceOs;
    private String deviceOsVersion;
    private int displayHeight;
    private int displayWidth;
    private String inputDevices;
    private String browser;
    private String browserVersion;
    private boolean isTablet;
    private boolean isWirelessDevice;
    private boolean isCrawler;
    private boolean isDesktop;
    private boolean ajaxSupportJavascript;


    public DeviceClassification() {}

    public DeviceClassification(String userAgent) {
        classifyUseragent(userAgent);
    }

    /**
     *
     * @param userAgent
     */
    public final DeviceClassification classifyUseragent(final String userAgent) {
        final Map result  = classifyUA(userAgent);
        setVendor(result.get("vendor"));
        setModel(result.get("model"));
        setParentId(result.get("parentId"));
        setInputDevices(result.get("inputDevices"));
        setDeviceOs(result.get("device_os"));
        setDeviceOsVersion(result.get("device_os_version"));
        setDisplayHeight(result.get("displayHeight"));
        setDisplayWidth(result.get("displayWidth"));
        setAjaxSupportJavascript(result.get("ajax_support_javascript"));
        setIsWirelessDevice(result.get("is_wireless_device"));
        setIsTablet(result.get("is_tablet"));
        setIsCrawler(result.get("is_crawler"));
        setIsDesktop(result.get("is_desktop"));
        setBrowser(result.get("browser"));
        setBrowserVersion(result.get("browser_version"));
        return this;
    }


    /**
     * Gets the vendor.
     *
     * @return the vendor
     */
    public final String getVendor() {
        return vendor;
    }

    /**
     * Sets the vendor.
     *
     * @param vendor the new vendor
     */
    private void setVendor(final Object vendor) {
        this.vendor = (String) vendor;
    }

    /**
     * Gets the model.
     *
     * @return the model
     */
    public final String getModel() {
        return model;
    }

    /**
     * Sets the model.
     *
     * @param model the new model
     */
    private void setModel(final Object model) {
        this.model = (String) model;
    }

    /**
     * Gets the parent id.
     *
     * @return the parent id
     */
    public final String getParentId() {
        return parentId;
    }

    /**
     * Sets the parent id.
     *
     * @param parentId the new parent id
     */
    private void setParentId(final Object parentId) {
        this.parentId = (String) parentId;
    }

    /**
     * Gets the input devices.
     *
     * @return the input devices
     */
    public final String getInputDevices() {
        return inputDevices;
    }

    /**
     * Sets the input devices.
     *
     * @param inputDevices the new input devices
     */
    private void setInputDevices(final Object inputDevices) {
        this.inputDevices = (String) inputDevices;
    }

    /**
     * Gets the display height.
     *
     * @return the display height
     */
    public final int getDisplayHeight() {
        return displayHeight;
    }

    /**
     * Sets the display height.
     *
     * @param displayHeight the new display height
     */
    private void setDisplayHeight(final Object displayHeight) {
        this.displayHeight = displayHeight == null ? 0 : Integer.valueOf((String) displayHeight);
    }

    /**
     * Gets the display width.
     *
     * @return the display width
     */
    public final int getDisplayWidth() {
        return displayWidth;
    }

    /**
     * Sets the display width.
     *
     * @param displayWidth the new display width
     */
    private void setDisplayWidth(final Object displayWidth) {
        this.displayWidth = displayWidth == null ? 0 : Integer.valueOf((String) displayWidth);
    }

    /**
     * Gets the device_os.
     *
     * @return the device_os
     */
    public final String getDeviceOs() {
        return deviceOs;
    }

    /**
     * Sets the device_os.
     *
     * @param deviceOs the new device_os
     */
    private void setDeviceOs(final Object deviceOs) {
        this.deviceOs = (String) deviceOs;
    }

    /**
     * Gets the device_os_version.
     *
     * @return the device_os_version
     */
    public final String getDeviceOsVersion() {
        return deviceOsVersion;
    }

    /**
     * Sets the device_os_version.
     *
     * @param deviceOsVersion the new device_os_version
     */
    private void setDeviceOsVersion(final Object deviceOsVersion) {
        this.deviceOsVersion = (String) deviceOsVersion;
    }

    /**
     * Gets the ajax_support_javascript.
     *
     * @return the ajaxSupportJavascript
     */
    public final boolean getAjaxSupportJavascript() {
        return ajaxSupportJavascript;
    }

    /**
     * Sets the ajax_support_javascript.
     *
     * @param ajaxSupportJavascript the new ajax_support_javascript
     */
    private void setAjaxSupportJavascript(final Object ajaxSupportJavascript) {
        this.ajaxSupportJavascript = Boolean.valueOf((String) ajaxSupportJavascript);
    }

    /**
     * Gets the is_tablet.
     *
     * @return the is_tablet
     */
    public final boolean getIsTablet() {
        return isTablet;
    }

    /**
     * Sets the is_tablet.
     *
     * @param isTablet the new is_tablet
     */
    private void setIsTablet(final Object isTablet) {
        this.isTablet = Boolean.valueOf((String) isTablet);
    }

    /**
     * Gets the checks if is wireless_device.
     *
     * @return the checks if is wireless_device
     */
    public final boolean getIsWirelessDevice() {
        return isWirelessDevice;
    }

    /**
     * Sets the is_wireless_device.
     *
     * @param isWirelessDevice the new is_wireless_device
     */
    private void setIsWirelessDevice(final Object isWirelessDevice) {
        this.isWirelessDevice = Boolean.valueOf((String) isWirelessDevice);
    }

    /**
     * Gets the is_crawler.
     *
     * @return the getIsCrawler
     */
    public final boolean getIsCrawler() {
        return isCrawler;
    }

    /**
     * Sets the is_crawler.
     *
     * @param isCrawler the new is_crawler
     */
    private void setIsCrawler(final Object isCrawler) {
        this.isCrawler = Boolean.valueOf((String) isCrawler);
    }

    /**
     * Gets the is_desktop.
     *
     * @return the is_desktop
     */
    public final boolean getIsDesktop() {
        return isDesktop;
    }

    /**
     * Sets the is_desktop.
     *
     * @param isDesktop the new is_desktop
     */
    private void setIsDesktop(final Object isDesktop) {
        this.isDesktop = Boolean.valueOf((String) isDesktop);
    }

    public String getBrowser() {
        return browser;
    }

    private void setBrowser(final Object browser) {
        this.browser = (String) browser;
    }

    public String getBrowserVersion() {
        return browserVersion;
    }

    private void setBrowserVersion(final Object browserVersion) {
        this.browserVersion = (String) browserVersion;
    }



    /* * * * Debugging Utilities * * * */


    /**
     * Prints the properties of to stdout.
     */
    final void print() {
        System.out.println("vendor                  = " + getVendor());
        System.out.println("model                   = " + getModel());
        System.out.println("parentId                = " + getParentId());
        System.out.println("inputDevices            = " + getInputDevices());
        System.out.println("device_os               = " + getDeviceOs());
        System.out.println("device_os_version       = " + getDeviceOsVersion());
        System.out.println("displayHeight           = " + getDisplayHeight());
        System.out.println("displayWidth            = " + getDisplayWidth());
        System.out.println("ajax_support_javascript = " + ajaxSupportJavascript);
        System.out.println("is_tablet               = " + getIsTablet());
        System.out.println("is_wireless_device      = " + getIsWirelessDevice());
        System.out.println("is_crawler              = " + getIsCrawler());
        System.out.println("is_desktop              = " + getIsDesktop());
        System.out.println("browser                 = " + getBrowser());
        System.out.println("browser_version         = " + getBrowserVersion());
    }

    private static void debugOutputdClass(final String userAgent) {
        final Map result = classifyUA(userAgent);
        final Iterator it = result.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = (Map.Entry) it.next();
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }

}
