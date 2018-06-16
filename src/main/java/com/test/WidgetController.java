package com.test;

import com.transport.lib.zookeeper.ZKUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RequestMapping("/api/**")
@RestController
public class WidgetController {

    static {
        ZKUtils.connect("localhost");
    }

    @Autowired
    PersonServiceTransport personService;

    @RequestMapping(method = RequestMethod.GET)
    public String index() {
        Integer id = personService.add("James Carr", "james@zapier.com", null).withTimeout(10_000).execute();
        System.out.printf("Resulting id is %s", id);
        System.out.println();
        Person person = personService.get(id).execute();
        System.out.println(person);
        personService.lol().execute();
        personService.lol2("kek").execute();
        System.out.println("Name: "  + personService.getName().execute());
        return "lol";
    }
}
