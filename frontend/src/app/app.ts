import { Component } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import {FooterComponent} from "./components/footer/footer.component";
import {HeaderComponent} from "./components/header/header.component";

@Component({
  selector: 'app-root',
    imports: [RouterOutlet, HeaderComponent, FooterComponent],
  templateUrl: 'app.html',
  styleUrl: 'app.less'
})
export class App {
}
