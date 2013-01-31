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
import java.util.Map.Entry;

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

public class Result {

    /** The vendor. */
    private String vendor;

    /**
     * Gets the vendor.
     *
     * @return the vendor
     */
    private String getVendor() {
        return vendor;
    }

    /**
     * Sets the vendor.
     *
     * @param vendor the new vendor
     */
    private void setVendor(String vendor) {
        this.vendor = vendor;
    }

    /**
     * Gets the model.
     *
     * @return the model
     */
    public String getModel() {
        return model;
    }

    /**
     * Sets the model.
     *
     * @param model the new model
     */
    private void setModel(String model) {
        this.model = model;
    }

    /**
     * Gets the parent id.
     *
     * @return the parent id
     */
    private String getParentId() {
        return parentId;
    }

    /**
     * Sets the parent id.
     *
     * @param parentId the new parent id
     */
    private void setParentId(String parentId) {
        this.parentId = parentId;
    }

    /**
     * Gets the input devices.
     *
     * @return the input devices
     */
    private String getInputDevices() {
        return inputDevices;
    }

    /**
     * Sets the input devices.
     *
     * @param inputDevices the new input devices
     */
    private void setInputDevices(String inputDevices) {
        this.inputDevices = inputDevices;
    }

    /**
     * Gets the display height.
     *
     * @return the display height
     */
    private int getDisplayHeight() {
        return displayHeight;
    }

    /**
     * Sets the display height.
     *
     * @param displayHeight the new display height
     */
    private void setDisplayHeight(int displayHeight) {
        this.displayHeight = displayHeight;
    }

    /**
     * Gets the display width.
     *
     * @return the display width
     */
    private int getDisplayWidth() {
        return displayWidth;
    }

    /**
     * Sets the display width.
     *
     * @param displayWidth the new display width
     */
    private void setDisplayWidth(int displayWidth) {
        this.displayWidth = displayWidth;
    }

    /**
     * Gets the device_os.
     *
     * @return the device_os
     */
    private String getDeviceOs() {
        return deviceOs;
    }

    /**
     * Sets the device_os.
     *
     * @param deviceOs the new device_os
     */
    private void setDeviceOs(String deviceOs) {
        this.deviceOs = deviceOs;
    }

    /**
     * Gets the ajax_support_javascript.
     *
     * @return the ajaxSupportJavascript
     */
    private boolean getAjaxSupportJavascript() {
        return ajaxSupportJavascript;
    }

    /**
     * Sets the ajax_support_javascript.
     *
     * @param ajaxSupportJavascript the new ajax_support_javascript
     */
    private void setAjaxSupportJavascript(boolean ajaxSupportJavascript) {
        this.ajaxSupportJavascript = ajaxSupportJavascript;
    }

    /**
     * Gets the is_tablet.
     *
     * @return the is_tablet
     */
    private boolean getIsTablet() {
        return isTablet;
    }

    /**
     * Sets the is_tablet.
     *
     * @param isTablet the new is_tablet
     */
    private void setIsTablet(boolean isTablet) {
        this.isTablet = isTablet;
    }

    /**
     * Gets the checks if is wireless_device.
     *
     * @return the checks if is wireless_device
     */
    private boolean getIsWirelessDevice() {
        return isWirelessDevice;
    }

    /**
     * Sets the is_wireless_device.
     *
     * @param isWirelessDevice the new is_wireless_device
     */
    private void setIsWirelessDevice(boolean isWirelessDevice) {
        this.isWirelessDevice = isWirelessDevice;
    }

    /**
     * Gets the is_crawler.
     *
     * @return the getIsCrawler
     */
    private boolean getIsCrawler() {
        return isCrawler;
    }

    /**
     * Sets the is_crawler.
     *
     * @param isCrawler the new is_crawler
     */
    private void setIsCrawler(boolean isCrawler) {
        this.isCrawler = isCrawler;
    }

    /**
     * Gets the is_desktop.
     *
     * @return the is_desktop
     */
    public boolean getIsDesktop() {
        return isDesktop;
    }

    /**
     * Sets the is_desktop.
     *
     * @param isDesktop the new is_desktop
     */
    private void setIsDesktop(boolean isDesktop) {
        this.isDesktop = isDesktop;
    }

    /** The model. */
    private String model;

    /** The parent id. */
    private String parentId;

    /** The input devices. */
    private String inputDevices;

    /** The display height. */
    private Integer displayHeight;

    /** The display width. */
    private Integer displayWidth;

    /** The device_os. */
    private String deviceOs;

    /** The ajax_support_javascript. */
    private boolean ajaxSupportJavascript;

    /** The is_tablet. */
    private boolean isTablet;

    /** The is_wireless_device. */
    private boolean isWirelessDevice;

    /** The is_crawler. */
    private boolean isCrawler;

    /** The is_desktop. */
    private boolean isDesktop;


    //UserAgentClassifier.destroyUA();

    DclassWrapper dw = new DclassWrapper();

    /**
     * Initiates UA JNI internal data
     */
    public Result() {
        dw.initUA();
    }

    public void classifyUseragent(String userAgent) {
        Map result  = dw.classifyUA(userAgent);
        Iterator it = result.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = (Map.Entry) it.next();
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }

//        vendor = null;
//        model = null;
//        parentId = null;
//        inputDevices = null;
//        deviceOs = null;
//        displayHeight =  null;
//        displayWidth =  null;
//        ajaxSupportJavascript =  null;
//        isTablet =  null;
//        isWirelessDevice =  null;
//        isCrawler =  null;
//        isDesktop =  null;

    /**
     * Prints the properties of to stdout.
     */
    public final void print() {
        System.out.println("vendor                  = " + getVendor());
        System.out.println("model                   = " + getModel());
        System.out.println("parentId                = " + getParentId());
        System.out.println("inputDevices            = " + getInputDevices());
        System.out.println("device_os               = " + getDeviceOs());
        System.out.println("displayHeight           = " + getDisplayHeight());
        System.out.println("displayWidth            = " + getDisplayWidth());
        System.out.println("ajax_support_javascript = "
                + ajaxSupportJavascript);
        System.out.println("is_tablet               = " + getIsTablet());
        System.out.println("is_wireless_device      = " + getIsWirelessDevice());
        System.out.println("is_crawler              = " + getIsCrawler());
        System.out.println("is_desktop              = " + getIsDesktop());
    }
}
