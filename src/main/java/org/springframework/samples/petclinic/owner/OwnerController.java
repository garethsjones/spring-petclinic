/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.samples.petclinic.owner;

import com.opencsv.CSVWriter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.samples.petclinic.owner.dao.PetsDao;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Juergen Hoeller
 * @author Ken Krebs
 * @author Arjen Poutsma
 * @author Michael Isvy
 */
@Controller
class OwnerController {

    private static final Logger log = LoggerFactory.getLogger(OwnerController.class);

    private static final String VIEWS_OWNER_CREATE_OR_UPDATE_FORM = "owners/createOrUpdateOwnerForm";
    private static final int WAIT = 250;

    private final OwnerRepository owners;
    private final PetsDao petsDao;

    @Autowired
    public OwnerController(OwnerRepository clinicService, PetsDao petsDao) {
        this.owners = clinicService;
        this.petsDao = petsDao;
    }

    @InitBinder
    public void setAllowedFields(WebDataBinder dataBinder) {
        dataBinder.setDisallowedFields("id");
    }

    @GetMapping("/owners/new")
    public String initCreationForm(Map<String, Object> model) {
        Owner owner = new Owner();
        model.put("owner", owner);
        return VIEWS_OWNER_CREATE_OR_UPDATE_FORM;
    }

    @PostMapping("/owners/new")
    public String processCreationForm(@Valid Owner owner, BindingResult result) {
        if (result.hasErrors()) {
            return VIEWS_OWNER_CREATE_OR_UPDATE_FORM;
        } else {
            this.owners.save(owner);
            return "redirect:/owners/" + owner.getId();
        }
    }

    @GetMapping("/owners/find")
    public String initFindForm(Map<String, Object> model) {
        model.put("owner", new Owner());
        return "owners/findOwners";
    }

    @GetMapping("/owners")
    public String processFindForm(Owner owner, BindingResult result, Map<String, Object> model) {

        // allow parameterless GET request for /owners to return all records
        if (owner.getLastName() == null) {
            owner.setLastName(""); // empty string signifies broadest possible search
        }

        // find owners by last name
        Collection<Owner> results = this.owners.findByLastName(owner.getLastName());
        if (results.isEmpty()) {
            // no owners found
            result.rejectValue("lastName", "notFound", "not found");
            return "owners/findOwners";
        } else if (results.size() == 1) {
            // 1 owner found
            owner = results.iterator().next();
            return "redirect:/owners/" + owner.getId();
        } else {
            // multiple owners found
            model.put("selections", results);
            return "owners/ownersList";
        }
    }

    @GetMapping("/owners/{ownerId}/edit")
    public String initUpdateOwnerForm(@PathVariable("ownerId") int ownerId, Model model) {
        Owner owner = this.owners.findById(ownerId);
        model.addAttribute(owner);
        return VIEWS_OWNER_CREATE_OR_UPDATE_FORM;
    }

    @PostMapping("/owners/{ownerId}/edit")
    public String processUpdateOwnerForm(@Valid Owner owner, BindingResult result, @PathVariable("ownerId") int ownerId) {
        if (result.hasErrors()) {
            return VIEWS_OWNER_CREATE_OR_UPDATE_FORM;
        } else {
            owner.setId(ownerId);
            this.owners.save(owner);
            return "redirect:/owners/{ownerId}";
        }
    }

    /**
     * Custom handler for displaying an owner.
     *
     * @param ownerId the ID of the owner to display
     * @return a ModelMap with the model attributes for the view
     */
    @GetMapping("/owners/{ownerId}")
    public ModelAndView showOwner(@PathVariable("ownerId") int ownerId) {
        ModelAndView mav = new ModelAndView("owners/ownerDetails");
        mav.addObject(this.owners.findById(ownerId));
        return mav;
    }

    @RequestMapping(value = "/pets.csv", method = RequestMethod.GET)
    public void pets(HttpServletResponse response) throws Exception {

        try (
            StringWriter writer = new StringWriter();

            CSVWriter csvWriter = new CSVWriter(writer,
                CSVWriter.DEFAULT_SEPARATOR,
                CSVWriter.NO_QUOTE_CHARACTER,
                CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                CSVWriter.DEFAULT_LINE_END)
        ) {
            String[] headerRecord = {"First name", "Last name", "Address", "City", "Telephone", "Pet", "Type", "Pet DoB", "Export date"};
            csvWriter.writeNext(headerRecord);

            List<List<String>> rows = petsDao.fetch();

            rows.stream()
                .map((row) -> {
                    try {
                        Thread.sleep(WAIT);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    return row.toArray(new String[]{});
                })
                .forEach(csvWriter::writeNext);

            response.setContentType("text/csv");
            InputStream is = new ByteArrayInputStream(writer.toString().getBytes());
            IOUtils.copy(is, response.getOutputStream());

            response.flushBuffer();
        }
    }

    @RequestMapping(value = "/pets-paginated.csv", method = RequestMethod.GET)
    public void petsPartition(HttpServletResponse response) throws Exception {

        try (
            StringWriter writer = new StringWriter();

            CSVWriter csvWriter = new CSVWriter(writer,
                CSVWriter.DEFAULT_SEPARATOR,
                CSVWriter.NO_QUOTE_CHARACTER,
                CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                CSVWriter.DEFAULT_LINE_END)
        ) {
            String[] headerRecord = {"First name", "Last name", "Address", "City", "Telephone", "Pet", "Type", "Pet DoB", "Export date"};
            csvWriter.writeNext(headerRecord);

            final AtomicInteger offset = new AtomicInteger(0);

            do {
                List<List<String>> rows = petsDao.fetch(1, offset.get());

                rows.stream()
                    .map((row) -> {
                        offset.incrementAndGet();
                        try {
                            Thread.sleep(WAIT);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        return row.toArray(new String[]{});
                    })
                    .forEach(csvWriter::writeNext);

                if (CollectionUtils.isEmpty(rows)) {
                    break;
                }
            } while (true);

            response.setContentType("text/csv");
            InputStream is = new ByteArrayInputStream(writer.toString().getBytes());
            IOUtils.copy(is, response.getOutputStream());

            response.flushBuffer();
        }
    }

    @RequestMapping(value = "/pets-stream.csv", method = RequestMethod.GET)
    public void petsStream(HttpServletResponse response) throws Exception {

        try (
            StringWriter writer = new StringWriter();

            CSVWriter csvWriter = new CSVWriter(writer,
                CSVWriter.DEFAULT_SEPARATOR,
                CSVWriter.NO_QUOTE_CHARACTER,
                CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                CSVWriter.DEFAULT_LINE_END)
        ) {
            String[] headerRecord = {"First name", "Last name", "Address", "City", "Telephone", "Pet", "Type", "Pet DoB", "Export date"};
            csvWriter.writeNext(headerRecord);

            petsDao.stream(writeRowFunction(csvWriter), Collectors.counting());

            response.setContentType("text/csv");
            InputStream is = new ByteArrayInputStream(writer.toString().getBytes());
            IOUtils.copy(is, response.getOutputStream());

            response.flushBuffer();
        }
    }

    private Function<List<String>, Integer> writeRowFunction(CSVWriter csvWriter) {

        return (row) -> {

            try {
                Thread.sleep(WAIT);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            csvWriter.writeNext(row.toArray(new String[]{}));

            return 1;
        };
    }

    @RequestMapping(value = "/pets-broken.csv", method = RequestMethod.GET)
    public void petsBroken(HttpServletResponse response) throws Exception {

        try (
            StringWriter writer = new StringWriter();

            CSVWriter csvWriter = new CSVWriter(writer,
                CSVWriter.DEFAULT_SEPARATOR,
                CSVWriter.NO_QUOTE_CHARACTER,
                CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                CSVWriter.DEFAULT_LINE_END)
        ) {
            String[] headerRecord = {"First name", "Last name", "Address", "City", "Telephone", "Pet", "Type", "Pet DoB", "Export date"};
            csvWriter.writeNext(headerRecord);

            petsDao.fetchStream()
                .map(writeRowFunction(csvWriter));

            response.setContentType("text/csv");
            InputStream is = new ByteArrayInputStream(writer.toString().getBytes());
            IOUtils.copy(is, response.getOutputStream());

            response.flushBuffer();
        }
    }
}
