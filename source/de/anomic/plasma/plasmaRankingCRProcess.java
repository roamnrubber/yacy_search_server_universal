// plasmaCRProcess.java
// -----------------------
// part of YaCy
// (C) by Michael Peter Christen; mc@anomic.de
// first published on http://www.anomic.de
// Frankfurt, Germany, 2005
// Created 15.11.2005
//
// $LastChangedDate: 2005-10-22 15:28:04 +0200 (Sat, 22 Oct 2005) $
// $LastChangedRevision: 968 $
// $LastChangedBy: theli $
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

package de.anomic.plasma;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import de.anomic.kelondro.kelondroAttrSeq;
import de.anomic.kelondro.kelondroBase64Order;
import de.anomic.server.serverFileUtils;
import de.anomic.server.serverDate;
import de.anomic.tools.bitfield;

public class plasmaRankingCRProcess {
    
    /*
    header.append("# Name=YaCy " + ((type.equals("crl")) ? "Local" : "Global") + " Citation Reference Ticket"); header.append((char) 13); header.append((char) 10);
    header.append("# Created=" + System.currentTimeMillis()); header.append((char) 13); header.append((char) 10);
    header.append("# Structure=<Referee-12>,'=',<UDate-3>,<VDate-3>,<LCount-2>,<GCount-2>,<ICount-2>,<DCount-2>,<TLength-3>,<WACount-3>,<WUCount-3>,<Flags-1>,'|',*<Anchor-" + ((type.equals("crl")) ? "6" : "12") + ">"); header.append((char) 13); header.append((char) 10);
    header.append("# ---"); header.append((char) 13); header.append((char) 10);
    */  

    private static boolean accumulate_upd(File f, kelondroAttrSeq acc) {
        // open file
        kelondroAttrSeq source_cr = null;
        try {
            source_cr = new kelondroAttrSeq(f, false);
        } catch (IOException e) {
            return false;
        }
        
        // put elements in accumulator file
        Iterator el = source_cr.keys();
        String key;
        kelondroAttrSeq.Entry new_entry, acc_entry;
        int FUDate, FDDate, LUDate, UCount, PCount, ACount, VCount, Vita;
        bitfield acc_flags, new_flags;
        while (el.hasNext()) {
            key = (String) el.next();
            new_entry = source_cr.getEntry(key);
            new_flags = new bitfield(kelondroBase64Order.enhancedCoder.encodeLong((long) new_entry.getAttr("Flags", 0), 1).getBytes());
            // enrich information with additional values
            if ((acc_entry = acc.getEntry(key)) != null) {
                FUDate = (int) acc_entry.getAttr("FUDate", 0);
                FDDate = (int) acc_entry.getAttr("FDDate", 0);
                LUDate = (int) acc_entry.getAttr("LUDate", 0);
                UCount = (int) acc_entry.getAttr("UCount", 0);
                PCount = (int) acc_entry.getAttr("PCount", 0);
                ACount = (int) acc_entry.getAttr("ACount", 0);
                VCount = (int) acc_entry.getAttr("VCount", 0);
                Vita   = (int) acc_entry.getAttr("Vita", 0);
                
                // update counters and dates
                acc_entry.setSeq(new_entry.getSeq()); // need to be checked
                
                UCount++; // increase update counter
                PCount += (new_flags.get(1)) ? 1 : 0;
                ACount += (new_flags.get(2)) ? 1 : 0;
                VCount += (new_flags.get(3)) ? 1 : 0;
                
                // 'OR' the flags
                acc_flags = new bitfield(kelondroBase64Order.enhancedCoder.encodeLong((long) acc_entry.getAttr("Flags", 0), 1).getBytes());
                for (int i = 0; i < 6; i++) {
                    if (new_flags.get(i)) acc_flags.set(i, true);
                }
                acc_entry.setAttr("Flags", (int) kelondroBase64Order.enhancedCoder.decodeLong(new String(acc_flags.getBytes())));
            } else {
                // initialize counters and dates
                acc_entry = acc.newEntry(key, new_entry.getAttrs(), new_entry.getSeq());
                FUDate = plasmaWordIndex.microDateHoursInt(System.currentTimeMillis()); // first update date
                FDDate = plasmaWordIndex.microDateHoursInt(System.currentTimeMillis()); // very difficult to compute; this is only a quick-hack
                LUDate = (int) new_entry.getAttr("VDate", 0);
                UCount = 0;
                PCount = (new_flags.get(1)) ? 1 : 0;
                ACount = (new_flags.get(2)) ? 1 : 0;
                VCount = (new_flags.get(3)) ? 1 : 0;
                Vita   = 0;
            }
            // make plausibility check?
            
            // insert into accumulator
            acc_entry.setAttr("FUDate", (long) FUDate);
            acc_entry.setAttr("FDDate", (long) FDDate);
            acc_entry.setAttr("LUDate", (long) LUDate);
            acc_entry.setAttr("UCount", (long) UCount);
            acc_entry.setAttr("PCount", (long) PCount);
            acc_entry.setAttr("ACount", (long) ACount);
            acc_entry.setAttr("VCount", (long) VCount);
            acc_entry.setAttr("Vita", (long) Vita);
            acc.putEntrySmall(acc_entry);
        }
        
        return true;
    }
    
    public static void accumulate(File from_dir, File tmp_dir, File err_dir, File bkp_dir, File to_file, int max_files) throws IOException {
        if (!(from_dir.isDirectory())) {
            System.out.println("source path " + from_dir + " is not a directory.");
            return;
        }
        if (!(tmp_dir.isDirectory())) {
            System.out.println("temporary path " + tmp_dir + " is not a directory.");
            return;
        }
        if (!(err_dir.isDirectory())) {
            System.out.println("error path " + err_dir + " is not a directory.");
            return;
        }
        if (!(bkp_dir.isDirectory())) {
            System.out.println("back-up path " + bkp_dir + " is not a directory.");
            return;
        }
        
        // open target file
        kelondroAttrSeq acc = null;
        if (!(to_file.exists())) {
            acc = new kelondroAttrSeq("Global Ranking Accumulator File",
                    "<Referee-12>,'='," +
                    "<UDate-3>,<VDate-3>,<LCount-2>,<GCount-2>,<ICount-2>,<DCount-2>,<TLength-3>,<WACount-3>,<WUCount-3>,<Flags-1>," +
                    "<FUDate-3>,<FDDate-3>,<LUDate-3>,<UCount-2>,<PCount-2>,<ACount-2>,<VCount-2>,<Vita-2>," +
                    "'|',*<Anchor-12>", false);
            acc.toFile(to_file);
        }
        acc = new kelondroAttrSeq(to_file, false);
        
        // collect source files
        File source_file = null;
        String[] files = from_dir.list();
        if (files.length < max_files) max_files = files.length;
        for (int i = 0; i < max_files; i++) {
            // open file
            source_file = new File(from_dir, files[i]);
            if (accumulate_upd(source_file, acc)) {
                // move cr file to temporary folder
                source_file.renameTo(new File(tmp_dir, files[i]));
            } else {
                // error case: the cr-file is not valid; move to error path
                source_file.renameTo(new File(err_dir, files[i]));
            }
        }
        
        // save accumulator to temporary file
        File tmp_file;
        if (to_file.toString().endsWith(".gz")) {
            tmp_file = new File(to_file.toString() + "." + (System.currentTimeMillis() % 1000) + ".tmp.gz");
        } else {
            tmp_file = new File(to_file.toString() + "." + (System.currentTimeMillis() % 1000) + ".tmp");
        }
        try {
            acc.toFile(tmp_file);
            // since this was successful, we remove the old file and move the new file to it
            to_file.delete();
            tmp_file.renameTo(to_file);
            serverFileUtils.moveAll(tmp_dir, bkp_dir);
        } catch (IOException e) {
            // move previously processed files back
            serverFileUtils.moveAll(tmp_dir, from_dir);
        }
        
    }
    
    public static int genrci(File cr_in, File rci_out) throws IOException {
        if (!(cr_in.exists())) return 0;
        kelondroAttrSeq cr = new kelondroAttrSeq(cr_in, false);
        //if (rci_out.exists()) rci_out.delete(); // we want only fresh rci here (during testing) 
        if (!(rci_out.exists())) {
            kelondroAttrSeq rcix = new kelondroAttrSeq("Global Ranking Reverse Citation Index",
                    "<AnchorDom-6>,'='," +
                    "<UDate-3>," +
                    "'|',*<Referee-12>", false);
            rcix.toFile(rci_out);
        }
        final kelondroAttrSeq rci = new kelondroAttrSeq(rci_out, false);
        
        // loop over all referees
        int count = 0;
        int size = cr.size();
        long start = System.currentTimeMillis();
	long l;
        final Iterator i = cr.keys();
        String referee, anchor, anchorDom;
        kelondroAttrSeq.Entry cr_entry, rci_entry;
        long cr_UDate, rci_UDate;
        while (i.hasNext()) {
            referee = (String) i.next();
            cr_entry = cr.getEntry(referee);
            cr_UDate = cr_entry.getAttr("UDate", 0);
            
            // loop over all anchors
            Iterator j = cr_entry.getSeq().entrySet().iterator();
            Map.Entry entry;
            while (j.hasNext()) {
                // get domain of anchors
                entry = (Map.Entry) j.next();
                anchor = (String) entry.getKey();
                if (anchor.length() == 6) anchorDom = anchor; else anchorDom = anchor.substring(6);

                // update domain-specific entry
                rci_entry = rci.getEntry(anchorDom);
                if (rci_entry == null) rci_entry = rci.newEntry(anchorDom, false);
                rci_entry.addSeq(referee, null);
                
                // update Update-Date
                rci_UDate = rci_entry.getAttr("UDate", 0);
                if (cr_UDate > rci_UDate) rci_entry.setAttr("UDate", cr_UDate);
                
                // insert entry
                rci.putEntry(rci_entry);
            }
            count++;
            if ((count % 1000) == 0) {
                l = java.lang.Math.max(1, (System.currentTimeMillis() - start) / 1000);
                System.out.println("processed " + count + " citations, " + (count / l) + " per second, rci.size = " + rci.size() + ", " + ((size - count) / (count / l)) + " seconds remaining; mem = " + Runtime.getRuntime().freeMemory());  
            }
            i.remove();
        }

        // finished. write to file
        cr = null;
        cr_in = null;
        System.gc();
        rci.toFile(rci_out);
        return count;
    }
    
    public static void main(String[] args) {
        // java -classpath source de.anomic.plasma.kelondroPropFile -transcode DATA/RANKING/GLOBAL/CRG-test-unsorted-original.cr DATA/RANKING/GLOBAL/CRG-test-generated.cr
        try {
            if ((args.length == 5) && (args[0].equals("-accumulate"))) {
                accumulate(new File(args[1]), new File(args[2]), new File(args[3]), new File(args[4]), new File(args[5]), Integer.parseInt(args[6]));
            }
            if ((args.length == 2) && (args[0].equals("-accumulate"))) {
                File root_path = new File(args[1]);
                File from_dir = new File(root_path, "DATA/RANKING/GLOBAL/014_othercr");
                File ready_dir = new File(root_path, "DATA/RANKING/GLOBAL/015_ready");
                File tmp_dir = new File(root_path, "DATA/RANKING/GLOBAL/016_tmp");
                File err_dir = new File(root_path, "DATA/RANKING/GLOBAL/017_err");
                File acc_dir = new File(root_path, "DATA/RANKING/GLOBAL/018_acc");
                String filename = "CRG-a-" + new serverDate().toShortString(true) + ".cr.gz";
                File to_file = new File(root_path, "DATA/RANKING/GLOBAL/020_con0/" + filename);
                if (!(ready_dir.exists())) ready_dir.mkdirs();
                if (!(tmp_dir.exists())) tmp_dir.mkdirs();
                if (!(err_dir.exists())) err_dir.mkdirs();
                if (!(acc_dir.exists())) acc_dir.mkdirs();
                if (!(to_file.getParentFile().exists())) to_file.getParentFile().mkdirs();
                serverFileUtils.moveAll(from_dir, ready_dir);
                long start = System.currentTimeMillis();
                int files = ready_dir.list().length;
                accumulate(ready_dir, tmp_dir, err_dir, acc_dir, to_file, 1000);
                long seconds = java.lang.Math.max(1, (System.currentTimeMillis() - start) / 1000);
                System.out.println("Finished accumulate for " + files + " files in " + seconds + " seconds (" + (files / seconds) + " files/second)");
            }
            if ((args.length == 3) && (args[0].equals("-recycle"))) {
                File root_path = new File(args[1]);
                int max_age_hours = Integer.parseInt(args[2]);
                File own_dir = new File(root_path, "DATA/RANKING/GLOBAL/010_owncr");
                File acc_dir = new File(root_path, "DATA/RANKING/GLOBAL/018_acc");
                File bkp_dir = new File(root_path, "DATA/RANKING/GLOBAL/019_bkp");
                if (!(own_dir.exists())) return;
                if (!(acc_dir.exists())) return;
                if (!(bkp_dir.exists())) bkp_dir.mkdirs();
                String[] list = acc_dir.list();
                long start = System.currentTimeMillis();
                int files = list.length;
                long d;
                File f;
                for (int i = 0; i < list.length; i++) {
                    f = new File(acc_dir, list[i]);
                    try {
                        d = (System.currentTimeMillis() - (new kelondroAttrSeq(f, false)).created()) / 3600000;
                        if (d > max_age_hours) {
                            // file is considered to be too old, it is not recycled
                            System.out.println("file " + f.getName() + " is old (" + d + " hours) and not recycled, only moved to backup");
                            f.renameTo(new File(bkp_dir, list[i]));
                        } else {
                            // file is fresh, it is duplicated and moved to be transferred to other peers again
                            System.out.println("file " + f.getName() + " is fresh (" + d + " hours old), recycled and moved to backup");
                            serverFileUtils.copy(f, new File(own_dir, list[i]));
                            f.renameTo(new File(bkp_dir, list[i]));
                        }
                    } catch (IOException e) {
                        // there is something wrong with this file; delete it
                        System.out.println("file " + f.getName() + " is corrupted and deleted");
                        f.delete();
                    }
                }
                long seconds = java.lang.Math.max(1, (System.currentTimeMillis() - start) / 1000);
                System.out.println("Finished recycling of " + files + " files in " + seconds + " seconds (" + (files / seconds) + " files/second)");
            }
            if ((args.length == 2) && (args[0].equals("-genrci"))) {
                File root_path = new File(args[1]);
                File cr_filedir = new File(root_path, "DATA/RANKING/GLOBAL/020_con0");
                File rci_file = new File(root_path, "DATA/RANKING/GLOBAL/030_rci0/RCI-0.rci.gz");
                rci_file.getParentFile().mkdirs();
                String[] cr_filenames = cr_filedir.list();
                for (int i = 0; i < cr_filenames.length; i++) {
                    long start = System.currentTimeMillis();
                    int count = genrci(new File(cr_filedir, cr_filenames[i]), rci_file);
                    long seconds = java.lang.Math.max(1, (System.currentTimeMillis() - start) / 1000);
                    System.out.println("Completed RCI generation for input file " + cr_filenames[i] + ": " + count + " citation references in " + seconds + " seconds (" + (count / seconds) + " CR-records/second)");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    /*
      Class-A File format:
      
      UDate  : latest update timestamp of the URL (as virtual date, hours since epoch)
      VDate  : last visit timestamp of the URL (as virtual date, hours since epoch)
      LCount : count of links to local resources
      GCount : count of links to global resources
      ICount : count of links to images (in document)
      DCount : count of links to other documents
      TLength: length of the plain text content (bytes)
      WACount: total number of all words in content
      WUCount: number of unique words in content (removed doubles)
      Flags  : Flags (0=update, 1=popularity, 2=attention, 3=vote)
     
      Class-a File format is an extension of Class-A plus the following attributes
      FUDate : first update timestamp of the URL
      FDDate : first update timestamp of the domain
      LUDate : latest update timestamp of the URL
      UCount : Update Counter (of 'latest update timestamp')
      PCount : Popularity Counter (proxy clicks)
      ACount : Attention Counter (search result clicks)
      VCount : Votes
      Vita   : Vitality (normed number of updates per time)
     */
}
