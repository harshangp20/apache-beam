package model;

import lombok.*;

import java.io.Serializable;

@ToString
@AllArgsConstructor
@NoArgsConstructor
@Data
public class CustomerEntity implements Serializable {

    private String id;
    private String name;
    private String age;
    private String birthDate;

}
