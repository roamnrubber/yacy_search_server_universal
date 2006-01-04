// plasmaIndexEntryContainer.java 
// ------------------------------
// part of YaCy
// (C) by Michael Peter Christen; mc@anomic.de
// first published on http://www.anomic.de
// Frankfurt, Germany, 2005
// last major change: 07.05.2005
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
// Using this software in any meaning (reading, learning, copying, compiling,
// running) means that you agree that the Author(s) is (are) not responsible
// for cost, loss of data or any harm that may be caused directly or indirectly
// by usage of this softare or this documentation. The usage of this software
// is on your own risk. The installation and usage (starting/running) of this
// software may allow other people or application to access your computer and
// any attached devices and is highly dependent on the configuration of the
// software which must be done by the user of the software; the author(s) is
// (are) also not responsible for proper configuration and usage of the
// software, even if provoked by documentation provided together with
// the software.
//
// Any changes to this file according to the GPL as documented in the file
// gpl.txt aside this file in the shipment you received can be done to the
// lines that follows this copyright notice here, but changes must not be
// done inside the copyright notive above. A re-distribution must contain
// the intact and unchanged copyright notice.
// Contributions and changes to the program code must be marked as such.


/*
    an indexContainer is a bag of indexEntries for a single word
    such an container represents a RWI snippet:
    it collects a new RWI until it is so big that it should be flushed to either
    - an indexAssortment: collection of indexContainers of same size or
    - the backend storage
 
    the creationTime is necessary to organize caching of containers
*/

package de.anomic.plasma;

import java.util.HashMap;
import java.util.Iterator;

import de.anomic.kelondro.kelondroBase64Order;

public final class plasmaWordIndexEntryContainer implements Comparable {

    private final String wordHash;
    private final HashMap container; // urlHash/plasmaWordIndexEntry - Mapping
    private long updateTime;
    
    public plasmaWordIndexEntryContainer(String wordHash) {
        this(wordHash,16);
    }
    
    public plasmaWordIndexEntryContainer(String wordHash, int initContainerSize) {
        this.wordHash = wordHash;
        this.updateTime = 0;
        container = new HashMap(initContainerSize); // a urlhash/plasmaWordIndexEntry - relation
    }
    
    public int size() {
        return container.size();
    }
    
    public long updated() {
        return updateTime;
    }
    
    public String wordHash() {
        return wordHash;
    }

    public int add(plasmaWordIndexEntry entry, long updateTime) {
        this.updateTime = java.lang.Math.max(this.updateTime, updateTime);
        return (add(entry)) ? 1 : 0;
    }
    
    public int add(plasmaWordIndexEntry[] entries, long updateTime) {
        int c = 0;
        for (int i = 0; i < entries.length; i++) if (add(entries[i])) c++;
        this.updateTime = java.lang.Math.max(this.updateTime, updateTime);
        return c;
    }
    
    public int add(plasmaWordIndexEntryContainer c) {
        // returns the number of new elements
        Iterator i = c.entries();
        int x = 0;
        while (i.hasNext()) {
            if (add((plasmaWordIndexEntry) i.next())) x++;
        }
        this.updateTime = java.lang.Math.max(this.updateTime, c.updateTime);
        return x;
    }

    private boolean add(plasmaWordIndexEntry entry) {
        // returns true if the new entry was added, false if it already existet
        return (container.put(entry.getUrlHash(), entry) == null);
    }

    public boolean contains(String urlHash) {
        return container.containsKey(urlHash);
    }

    public plasmaWordIndexEntry[] getEntryArray() {
        return (plasmaWordIndexEntry[]) container.values().toArray();
    }

    public Iterator entries() {
        // returns an iterator of plasmaWordIndexEntry objects
        return container.values().iterator();
    }

    public static plasmaWordIndexEntryContainer instantContainer(String wordHash, long creationTime, plasmaWordIndexEntry entry) {
        plasmaWordIndexEntryContainer c = new plasmaWordIndexEntryContainer(wordHash,1);
        c.add(entry);
        c.updateTime = creationTime;
        return c;
    }

    public String toString() {
        return "C[" + wordHash + "] has " + container.size() + " entries";
    }
    
    public int compareTo(Object obj) {
        plasmaWordIndexEntryContainer other = (plasmaWordIndexEntryContainer) obj;
        return this.wordHash.compareTo(other.wordHash);
    }

    public int hashCode() {
        return (int) kelondroBase64Order.enhancedCoder.decodeLong(this.wordHash.substring(0, 4));
    }
    
}
